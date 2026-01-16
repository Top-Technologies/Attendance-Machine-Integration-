from odoo import models, fields, api
from datetime import datetime, timedelta

class HikvisionAttendance(models.Model):
    _name = 'hikvision.attendance'
    _description = 'Hikvision Daily Attendance Summary'
    _order = 'date desc'

    employee_id = fields.Many2one('hr.employee', string="Employee", required=True)
    date = fields.Date(string="Date", required=True)
    first_check_in = fields.Datetime(string="First Check-in")
    last_check_out = fields.Datetime(string="Last Check-out")
    status = fields.Selection([
        ('in', 'Checked In'),
        ('out', 'Checked Out'),
    ], string="Punch Status")
    
    # Attendance status
    attendance_status = fields.Selection([
        ('present', 'Present'),
        ('incomplete', 'Incomplete'),
        ('absent', 'Absent'),
    ], string="Attendance", compute="_compute_attendance_status", store=True)
    
    # Computed fields
    total_hours = fields.Float(string="Total Hours", compute="_compute_total_hours", store=True)
    is_late = fields.Boolean(string="Late", compute="_compute_late_early", store=True)
    is_early_leave = fields.Boolean(string="Early Leave", compute="_compute_late_early", store=True)
    late_minutes = fields.Integer(string="Late (min)", compute="_compute_late_early", store=True)
    early_leave_minutes = fields.Integer(string="Early (min)", compute="_compute_late_early", store=True)
    
    # Overtime & Approval
    overtime_hours = fields.Float(string="Overtime Hours", compute="_compute_overtime", store=True)
    ot_payable_hours = fields.Float(string="Payable OT Hours", compute="_compute_overtime", store=True, help="Weighted OT hours based on policy rates")
    
    # Approval Stats (Non-stored or stored for search? Stored is better for perf)
    ot_weekly_total = fields.Float(string="Weekly OT Total", compute="_compute_approval_stats")
    ot_monthly_total = fields.Float(string="Monthly OT Total", compute="_compute_approval_stats")
    
    approval_state = fields.Selection([
        ('draft', 'Draft'),
        ('to_approve', 'Pending'),
        ('second_approval', 'Second Approval'),
        ('approved', 'Approved'),
        ('refused', 'Refused')
    ], string="Approval Status", default='draft', tracking=True)

    @api.depends('first_check_in', 'last_check_out')
    def _compute_attendance_status(self):
        for rec in self:
            if rec.first_check_in and rec.last_check_out:
                rec.attendance_status = 'present'
            elif rec.first_check_in and not rec.last_check_out:
                rec.attendance_status = 'incomplete'
            else:
                rec.attendance_status = 'absent'

    @api.depends('first_check_in', 'last_check_out')
    def _compute_total_hours(self):
        for rec in self:
            if rec.first_check_in and rec.last_check_out:
                delta = rec.last_check_out - rec.first_check_in
                rec.total_hours = delta.total_seconds() / 3600
            else:
                rec.total_hours = 0.0

    @api.depends('first_check_in', 'last_check_out', 'employee_id.attendance_policy_id', 'date')
    def _compute_late_early(self):
        import pytz
        for rec in self:
            rec.is_late = False
            rec.is_early_leave = False
            rec.late_minutes = 0
            rec.early_leave_minutes = 0
            
            if not rec.employee_id or not rec.date:
                continue
            
            policy = rec.employee_id.attendance_policy_id
            if not policy or policy.ignore_late_early:
                continue
                
            tz_name = rec.employee_id.tz or 'UTC'
            try:
                local_tz = pytz.timezone(tz_name)
            except:
                local_tz = pytz.UTC
            
            # Helper to convert UTC datetime to local float hour
            def get_local_hour(dt_utc):
                if not dt_utc: return 0.0
                dt_local = dt_utc.astimezone(local_tz)
                return dt_local.hour + dt_local.minute / 60.0

            if rec.first_check_in:
                check_in_hour = get_local_hour(rec.first_check_in)
                limit = policy.work_start + (policy.late_tolerance / 60.0)
                if check_in_hour > limit:
                    rec.is_late = True
                    rec.late_minutes = int((check_in_hour - policy.work_start) * 60)

            if rec.last_check_out:
                check_out_hour = get_local_hour(rec.last_check_out)
                limit = policy.work_end - (policy.early_leave_tolerance / 60.0)
                if check_out_hour < limit:
                    rec.is_early_leave = True
                    rec.early_leave_minutes = int((policy.work_end - check_out_hour) * 60)

    @api.depends('last_check_out', 'employee_id.attendance_policy_id')
    def _compute_overtime(self):
        import pytz
        for rec in self:
            rec.overtime_hours = 0.0
            rec.ot_payable_hours = 0.0
            
            policy = rec.employee_id.attendance_policy_id
            if not policy or not policy.ot_apply or not rec.last_check_out:
                continue
                
            tz_name = rec.employee_id.tz or 'UTC'
            try:
                local_tz = pytz.timezone(tz_name)
            except:
                local_tz = pytz.UTC
                
            dt_local = rec.last_check_out.astimezone(local_tz)
            check_out_hour = dt_local.hour + dt_local.minute / 60.0
            
            # Simple OT Calculation (Hours after work end?)
            # Usually OT is strictly (Check Out - Work End) or (Check Out - OT Start)
            # User previously had logic: if check_out > ot_start_time, OT = check_out - work_end
            
            # Calculate raw OT hours first
            raw_ot_hours = 0.0
            
            # Use policy logic for start
            ot_start = policy.ot_start_time # e.g. 17.51
            cutoff_hour = policy.work_end # e.g. 17.5
            
            # Handle day crossing
            is_next_day = False
            check_out_date = dt_local.date()
            if check_out_date > rec.date:
                is_next_day = True
                check_out_hour += 24.0 # Adjust for calculation
            
            if check_out_hour > ot_start:
                 raw_ot_hours = check_out_hour - cutoff_hour
                 # Cap at limit
                 limit = policy.ot_end_limit
                 if limit < 12.0: limit += 24.0 # Limit is usually next morning
                 if check_out_hour > limit:
                     raw_ot_hours = limit - cutoff_hour
            
            rec.overtime_hours = max(0.0, raw_ot_hours)

            # --- Calculate Payable Hours based on Rates ---
            if rec.overtime_hours > 0:
                payable = 0.0
                
                # Determine Day Type
                weekday = rec.date.weekday() # 0=Mon, 6=Sun
                is_holiday = False
                
                # Check Public Holidays
                if rec.employee_id.resource_calendar_id:
                     # Check global leaves
                     start_dt = datetime.combine(rec.date, datetime.min.time())
                     end_dt = datetime.combine(rec.date, datetime.max.time())
                     # Simply checking if any global leave intersects today
                     leaves = rec.employee_id.resource_calendar_id.global_leave_ids
                     for leave in leaves:
                         if leave.date_from.date() <= rec.date <= leave.date_to.date():
                             is_holiday = True
                             break
                
                # Base Rate determination
                rate = policy.rate_weekday
                if is_holiday:
                    rate = policy.rate_holiday
                elif weekday == 6: # Sunday
                    rate = policy.rate_sunday
                elif weekday == 5: # Saturday
                    rate = policy.rate_saturday
                
                # Special Time-based Logic (applied iteratively?)
                # We have a duration (raw_ot_hours) starting from `cutoff_hour`
                # We need to integrate this duration over time to apply rates
                
                # Simplification: Iterate hour by hour or handling intervals
                # Start: cutoff_hour (e.g. 17.5)
                # End: Start + raw_ot_hours
                
                current = cutoff_hour
                end = cutoff_hour + rec.overtime_hours
                
                # We step through 30min chunks? Or exact calculation?
                # Exact:
                
                # Intervals of interest:
                # 1. Saturday Afternoon (if Saturday) > saturday_afternoon_start
                # 2. Night > night_start (e.g. 22.0)
                # 3. Night < night_end (e.g. 6.0 next day -> 30.0)
                
                # Note: `current` might be > 24.0 (next day)
                
                # Sort transition points
                points = [current, end]
                
                # Night Start (e.g. 22)
                if policy.night_start > current and policy.night_start < end:
                    points.append(policy.night_start)
                
                # Sat Afternoon Start (e.g. 13) - Only if Saturday
                if weekday == 5 and not is_holiday:
                    if policy.saturday_afternoon_start > current and policy.saturday_afternoon_start < end:
                        points.append(policy.saturday_afternoon_start)

                points = sorted(list(set(points)))
                
                for i in range(len(points) - 1):
                    p_start = points[i]
                    p_end = points[i+1]
                    duration = p_end - p_start
                    
                    interval_rate = rate # Default for day
                    
                    # Apply specific overrides
                    
                    # 1. Saturday Afternoon override
                    if weekday == 5 and not is_holiday and p_start >= policy.saturday_afternoon_start:
                        interval_rate = max(interval_rate, policy.rate_saturday_afternoon)
                    
                    # 2. Night Rate override (High priority)
                    # Check if p_start is in night window (22 to 6+24=30)
                    night_s = policy.night_start
                    night_e = policy.night_end + 24.0 # simplifies next day logic
                    
                    # If simplified night is 22:00 -> 06:00
                    if p_start >= night_s:
                        interval_rate = max(interval_rate, policy.rate_night)
                    elif p_start < (policy.night_end): # Early morning OT? Unlikely with start=17.5, but possible
                         interval_rate = max(interval_rate, policy.rate_night)
                    
                    payable += duration * interval_rate
                
                rec.ot_payable_hours = payable

            # Trigger approval if OT exists
            if rec.overtime_hours > 0 and rec.approval_state == 'draft':
                rec.approval_state = 'to_approve'

    def _compute_approval_stats(self):
        for rec in self:
            # Weekly Total
            start_week = rec.date - timedelta(days=rec.date.weekday())
            end_week = start_week + timedelta(days=6)
            
            weekly_recs = self.search([
                ('employee_id', '=', rec.employee_id.id),
                ('date', '>=', start_week),
                ('date', '<=', end_week),
                ('id', '!=', rec.id) # Exclude current potentially? Or include? User wants "how many... does person have". Include current + others.
            ])
            # Just sum all in database (approved + to_approve?)
            # User wants to know "person have", usually implies 'Approved' or 'Worked'.
            # Let's sum all overtime_hours
            total_w = sum(weekly_recs.mapped('overtime_hours'))
            rec.ot_weekly_total = total_w + rec.overtime_hours

            # Monthly Total
            start_month = rec.date.replace(day=1)
            # End month logic skipped, just >= start_month and < next month
            # Simple: matching same month
            monthly_recs = self.search([
                ('employee_id', '=', rec.employee_id.id),
                ('date', '>=', start_month),
                ('date', '<', (start_month + timedelta(days=32)).replace(day=1)),
                ('id', '!=', rec.id)
            ])
            total_m = sum(monthly_recs.mapped('overtime_hours'))
            rec.ot_monthly_total = total_m + rec.overtime_hours

    def action_submit_ot(self):
        self.ensure_one()
        self.approval_state = 'to_approve'
        
    def action_first_approve(self):
        self.ensure_one()
        self.approval_state = 'second_approval'

    def action_second_approve(self):
        self.ensure_one()
        self.approval_state = 'approved'
        
    def action_refuse_ot(self):
        self.ensure_one()
        self.approval_state = 'refused'
        self.overtime_hours = 0.0
        self.ot_payable_hours = 0.0

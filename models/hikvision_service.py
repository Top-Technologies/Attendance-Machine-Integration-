from odoo import models, fields, api
from odoo.exceptions import UserError
import requests
from requests.auth import HTTPDigestAuth
import logging

_logger = logging.getLogger(__name__)

DEVICE_IP = "192.168.64.138"
DEVICE_USER = "admin"
DEVICE_PASS = "Carpedium1"


class HikvisionUser(models.Model):
    _name = "hikvision.user"
    _description = "Hikvision Employee"

    employee_id = fields.Char(string="Employee ID", required=True, copy=False)
    employee_no = fields.Char(string="Employee No", related='employee_id', store=False)  # Alias for compatibility
    name = fields.Char(string="Name")
    odoo_employee_id = fields.Many2one('hr.employee', string="Odoo Employee")


class HikvisionService(models.TransientModel):
    _name = "hikvision.service"
    _description = "Hikvision Device Service"

    def fetch_all_users(self):
        """Fetch users from Hikvision device using Digest Auth."""
        url = f"http://{DEVICE_IP}/ISAPI/AccessControl/UserInfo/Search?format=json"

        payload = {
            "UserInfoSearchCond": {
                "searchID": "1",
                "searchResultPosition": 0,
                "maxResults": 200
            }
        }

        try:
            response = requests.post(
                url,
                auth=HTTPDigestAuth(DEVICE_USER, DEVICE_PASS),
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=20
            )

            data = response.json()

        except Exception as e:
            raise UserError(f"Failed to communicate with device: {e}")

        user_list = data.get("UserInfoSearch", {}).get("UserInfo", [])

        User = self.env["hikvision.user"]

        for u in user_list:
            emp_id = u.get("employeeNo")
            name = u.get("name", "Unknown")

            if not emp_id:
                continue

            # check existing
            user = User.search([("employee_id", "=", emp_id)], limit=1)
            if user:
                user.write({"name": name})
            else:
                User.create({
                    "employee_id": emp_id,
                    "name": name
                })

        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": "Success",
                "message": "Users synced from device.",
                "type": "success",
            }
        }

    def action_sync_and_open_users(self):
        """Sync users from device and open the user list"""
        self.fetch_all_users()

        return {
            "type": "ir.actions.act_window",
            "name": "Device Users",
            "res_model": "hikvision.user",
            "view_mode": "tree,form",
        }

    @api.model
    def action_cron_fetch_all(self):
        """Scheduled action to fetch logs from all connected devices."""
        devices = self.env['hikvision.device'].search([('status', '!=', 'error')])
        for device in devices:
            try:
                _logger.info(f"Cron: Fetching logs for device {device.name}")
                # Fetch logs for today (or recent period if needed, but today is safest for freq runs)
                # If we want to catch up, we might fetch last 2 days etc.
                # using action_fetch_today_logs which fetches today
                device.action_fetch_today_logs()
            except Exception as e:
                _logger.error(f"Cron: Failed to fetch logs for {device.name}: {e}")

from elastalert.alerts import Alerter
from elastalert.util import elastalert_logger as log
import json
from urllib import request
import yaml


class FeishuAlert(Alerter):
    def __init__(self, rule):
        super(FeishuAlert, self).__init__(rule)
        self.url_staging = 'http://tj1-miui-micfc-ksc-vr1-10293-ko2v4abi4.kscn:8408/insidemsg/sendWarning'
        self.url_uat = 'http://10.20.65.2:8408//insidemsg/sendWarning'
        self.ual_pdt = 'http://loan-inside-msg.pdt.mixiaojin.srv:8408/insidemsg/sendWarning'

    def alert(self, match):
        """ Send an alert. Match is a dictionary of information about the alert.
        :param match: A dictionary of relevant information to the alert.
        """
        headers = {
            "Content-Type": "application/json"
        }

        req_body = self.get_req_body(match)
        data = bytes(json.dumps(req_body), encoding='utf8')
        req = request.Request(url=self.url_uat, data=data, headers=headers, method='POST')
        try:
            response = request.urlopen(req)
        except Exception as e:
            log.info(e.read().decode())
            return

        rsp_body = response.read().decode('utf-8')
        rsp_dict = json.loads(rsp_body)
        code = rsp_dict.get("code", -1)
        if code != 0:
            log.info("send message error, code = ", code, ", msg =", rsp_dict.get("msg", ""))

        return None

    def get_info(self):
        """ Returns a dictionary of data related to this alert. At minimum, this should contain
        a field type corresponding to the type of Alerter. """
        return {'type': 'feishu'}

    def get_content(self, match):
        log.log('告警入参match:' + match)
        content = {'emailContent': '', 'feishuContent': match.__str__()}
        return content

    def get_req_body(self, match):
        req_body = {
            'subject': self.get_subject(match),
            'content': self.get_content(match),
            'priority': 1,
            'receiverId': 'mixiaojin-rd-6'
        }
        return req_body

    def get_subject(self, match):
        return '告警测试-标题'


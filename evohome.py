

class MQTTCommand():
    ''' Object to hold details of command received via MQTT to be sent to evohome controller.'''
    def __init__(self, command_code=None, command_name=None, destination=None, args=None, serial_port=-1, send_mode="I", instruction=None):
        self.command_code = command_code
        self.command_name = command_name
        self.destination = destination
        self.args = args
        self.arg_desc = "[]"
        self.send_mode = send_mode
        self.send_string = None
        self.send_dtm = None
        self.retry_dtm = None
        self.retries = 0
        self.send_failed = False
        self.wait_for_ack = False
        self.reset_ports_on_fail = False
        self.send_acknowledged = False
        self.send_acknowledged_dtm = None
        self.dev1 = None
        self.dev2 = None
        self.dev3 = None
        self.payload = ""
        self.command_instruction = instruction


    def payload_length(self):
        if self.payload:
            return int(len(self.payload)/2)
        else:
            return 0

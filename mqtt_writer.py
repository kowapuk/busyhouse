from umqtt.robust import MQTTClient


class MQTTWriter:
  __slots__ = ('client')
  def __init__(self, id='1', server='io.adafruit.com', user='kowapuk',  password = '72f8caf1dea3400cb65da773e8614478', port=1883):
    self.client = MQTTClient(
    client_id=id, 
    server=server, 
    user=user, 
    password='72f8caf1dea3400cb65da773e8614478', 
    port=port
    )
    self._connect()

  def _connect(self):
    print("Connecting....."))
    self.client.connect()
    print("Connection successful")

  def on_next(self, topic, data):
    # data = bytes(json.dumps(x), 'utf-8')
    # self.client.publish(bytes(self.topic, 'utf-8'), data)
    self.client.publish(topic=topic, msg=data)

  def on_completed(self):
    print("mqtt_completed, disconnecting")
    self.client.disconnect()

  def on_error(self, e):
    print("mqtt on_error: %s, disconnecting" %e)
    self.client.disconnect()

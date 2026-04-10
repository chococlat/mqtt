import paho.mqtt.client as mqtt
import time
import threading
from enum import Enum
import uuid
import sys

from common.logging import CustomLogger
mainlogger = CustomLogger("common.mqtt.client")

class MQTTMode(Enum):
    PUBLISHER = "publisher"
    CONSUMER = "consumer"


class MQTTClient:
    def __init__(self, broker: str, port: int, mode: MQTTMode, qos: int = 2, client_uuid: str | None = None, username: str | None = None, password: str | None = None):
        if not isinstance(mode, MQTTMode):
            raise ValueError(f"Invalid MQTT mode: {mode}. Use MQTTMode.PUBLISHER or MQTTMode.CONSUMER.")
        self.broker = broker
        self.port = port
        self.mode = mode
        self.qos = qos
        self.client_uuid = client_uuid or str(uuid.uuid4())
        self.username = username
        self.password = password
        

        # use MQTT v5 callbacks
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.client_uuid, clean_session=False)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish
        self.client.enable_logger()
        
        if self.username is not None:
            self.client.username_pw_set(self.username, self.password)

        self._connected_event = threading.Event()
        self._loop_started = False
        self.topic = None

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            mainlogger.info(f"Connected to MQTT broker at {self.broker}:{self.port}")
            self._connected_event.set()
        else:
            mainlogger.info(f"[on_connect] Connection failed, rc={rc}")
            self._connected_event.clear()

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code=None, properties=None):
        mainlogger.info(f"[on_disconnect] Disconnected, reason_code={reason_code}")
        self._connected_event.clear()

    def _on_publish(self, client, userdata, mid, reason_code=None, properties=None):
        mainlogger.info(f"[on_publish] Message MID={mid} delivered (reason_code={reason_code})")

    def _on_message(self, client, userdata, msg):
        mainlogger.info(f"[on_message] Received: {msg.payload.decode()} on {msg.topic}")
        if hasattr(msg, "properties") and msg.properties:
            mainlogger.info(f"[on_message] Properties: {msg.properties}")
            
    def set_topic(self, topic: str):
        """
        Set the topic for publishing or subscribing.
        """
        self.topic = topic
        mainlogger.info(f"[set_topic] Topic set to '{self.topic}'")
        return self
    
    def refresh_connection(self):
        """
        Reconnect if disconnected.
        """
        if not self._connected_event.is_set():
            mainlogger.info("Refreshing MQTT connection...")
            self.connect()
        return self

    def connect(self):
        """
        Connect to the broker and start the network loop in background.
        Blocks until the connection is established.
        """
        mainlogger.info(f"Connecting to MQTT broker at {self.broker}:{self.port} as {self.client_uuid}...")
        while True:
            try:
                self._connected_event.clear()
                self.client.connect(self.broker, self.port)
                if not self._loop_started:
                    rc = self.client.loop_start()
                    self._loop_started = True
                    if rc != mqtt.MQTT_ERR_SUCCESS:
                        mainlogger.info(f"[connect] loop_start failed (rc={rc}), retrying in 5s…")
                        time.sleep(5)
                        continue

                if not self._connected_event.wait(timeout=10):
                    mainlogger.info("[connect] Timeout waiting for CONNACK, retrying in 5s…")
                    time.sleep(5)
                    continue

                # connected
                break

            except Exception as e:
                mainlogger.error(f"[connect] Exception: {e}, retrying in 5s…")
                time.sleep(5)
            except KeyboardInterrupt:
                mainlogger.debug("[connect] KeyboardInterrupt, exiting…")
                sys.exit(0)

        return self

    def publish(self, message: str, topic:str|None=None, wait_timeout: float | None = None):
        """
        Publish a message. Optionally block for up to wait_timeout seconds
        for the PUBACK/PUBCOMP handshake.
        """
        if not topic and self.topic:
            topic = self.topic
        elif not topic:
            raise ValueError("No topic provided for publishing. Use set_topic() or provide a topic argument.")
        
        info = self.client.publish(topic, message, qos=self.qos)
        try:
            if wait_timeout is not None:
                info.wait_for_publish(wait_timeout)
            else:
                info.wait_for_publish()
        except Exception:
            mainlogger.error(f"[publish] Exception waiting for publish to '{topic}'")
            return False

        if not info.is_published():
            mainlogger.error(f"[publish] Failed to confirm publish to '{topic}'")
            return False
            
    

    def consume_loop(self, topic: str | None = None, on_message_callback=None, block: bool = True):
        """
        Subscribe (if topic provided) and consume messages forever.
        Uses background loop; this method just keeps the process alive.
        """
        cb = on_message_callback or self._on_message
        self.client.on_message = cb
        
        if not topic:
            topic = self.topic

        if topic:
            self.client.subscribe(topic, qos=self.qos)
            print(f"[consume_loop] Subscribed to '{topic}' (QoS={self.qos})")

        mainlogger.info("[consume_loop] Entering receive loop…")
        if block:
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                self.disconnect()
                sys.exit(0)

    def disconnect(self):
        """
        Disconnect and stop the background loop.
        """
        if self.client.is_connected():
            self.client.disconnect()
        if self._loop_started:
            self.client.loop_stop()
            self._loop_started = False
        mainlogger.info("[disconnect] Clean shutdown")

"""
This is a sample client script to provide possibility to turn EV3 Robot in an economic agent.

"""

import logging
import time
import traceback
import typing as tp

from ast import literal_eval
from paho.mqtt.client import Client

logger = logging.getLogger(__name__)


class EV3Client:
    """
    Client class for offering jobs to the EV3 robot.

    """

    def __init__(self):

        """
        Insert MQTT broker address:port here
        """
        self.mqtt_broker: str = "127.0.0.1"
        self.mqtt_port: int = 1893

        """
        Insert your client ID
        """
        self.mqtt_client_id: str = "ev3"

        self.mqtt_topics: list = [("ev3_task", 0), ("ev3_report", 0)]
        self.mqtt_client: tp.Optional[Client] = None

        self.index: tp.Optional[int] = None
        self.tr_hash: tp.Optional[str] = None
        self.total_time: int = 0

    def connect_to_mqtt(self):
        """
        Connect to a MQTT broker. Set up subscribers.

        """

        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                logger.info("Connected to MQTT Broker!")
            else:
                logger.error("Failed to connect, return code %d\n", rc)
                raise Exception

        # Set Connecting Client ID
        self.mqtt_client: Client = Client(self.mqtt_client_id)
        self.mqtt_client.on_connect = on_connect
        self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port)

        self.mqtt_client.subscribe(self.mqtt_topics[0][0])
        self.mqtt_client.on_message = self.on_response

        self.mqtt_client.loop_forever()

        logger.info("Started MQTT subscriber.")

    def publish_offer(self, message: str):
        """
        Publish a message to MQTT topic.

        :param message: Message to send.

        """

        result = self.mqtt_client.publish(self.mqtt_topics[1][0], message)
        status = result[0]
        if status == 0:
            logger.info(f"Sent `{message}` to topic `{self.mqtt_topics[1][0]}`.")
        else:
            logger.error(f"Failed to send message to topic {self.mqtt_topics[1][0]}: {result}")

    def on_response(self, client, userdata, msg):
        """
        What to do on income MQTT message.

        :param client: MQTT client.
        :param userdata: MQTT userdata.
        :param msg: Income message.
        """

        try:
            route: dict = literal_eval(msg.payload.decode())
            time_values = [sublist[2:] for sublist in route]
            self.total_time: int = 0

            for i in time_values:
                self.total_time += i[0]
            time.sleep(self.total_time)

            self.publish_offer(f"Task done! Time spent for the task: {self.total_time}")


        except Exception:
            logger.info(f"Error parsing response: {traceback.format_exc()}")


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    ev3_client: EV3Client = EV3Client()
    ev3_client.connect_to_mqtt()

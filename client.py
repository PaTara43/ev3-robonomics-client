"""
This is a sample client script to provide possibility to turn EV3 Robot in an economic agent.

"""

import logging
import time
import traceback
import typing as tp

from ast import literal_eval
from os import getenv
from paho.mqtt.client import Client
from robonomicsinterface import Account, ipfs_get_content, ipfs_32_bytes_to_qm_hash, Liability, Subscriber, SubEvent
from threading import Thread

logger = logging.getLogger(__name__)


class EV3Client:
    """
    Client class for offering jobs to the EV3 robot.

    """

    def __init__(self):
        self.seed: str = getenv("SEED")
        self.user_acc: Account = Account(seed=self.seed)

        """
        Insert MQTT broker address:port here
        """
        self.mqtt_broker: str = "127.0.0.1"
        self.mqtt_port: int = 1893

        """
        Insert your client ID
        """
        self.mqtt_client_id: str = "user_patara"

        self.mqtt_topics: list = [("offer", 0), ("response", 0)]
        self.mqtt_client: tp.Optional[Client] = None

        self.index: tp.Optional[int] = None
        self.tr_hash: tp.Optional[str] = None

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

        self.mqtt_client.subscribe(self.mqtt_topics[1][0])
        self.mqtt_client.on_message = self.on_response

        self.mqtt_client.loop_forever()

        logger.info("Started MQTT subscriber.")

    def publish_offer(self, message: str):
        """
        Publish a message to MQTT topic.

        :param message: Message to send.

        """

        result = self.mqtt_client.publish(self.mqtt_topics[0][0], message)
        status = result[0]
        if status == 0:
            logger.info(f"Sent `{message}` to topic `{self.mqtt_topics[0][0]}`.")
        else:
            logger.error(f"Failed to send message to topic {self.mqtt_topics[0][0]}: {result}")

    def on_response(self, client, userdata, msg):
        """
        What to do on income MQTT message.

        :param client: MQTT client.
        :param userdata: MQTT userdata.
        :param msg: Income message.
        """

        try:
            response: dict = literal_eval(msg.payload.decode())
            if response["addr"] == self.user_acc.get_address():
                logger.info(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
                if response["res"]:
                    logger.info(f"Offer accepted, the agent has sent liability credentials, creating new liability.")
                    self.index, self.tr_hash = self.create_liability(
                        response["technics"], response["price"], response["ev3_addr"], response["signature"]
                    )
                    logger.info(f"New liability with index {self.index} created at {self.tr_hash}!")
        except Exception:
            logger.info(f"Error parsing response: {traceback.format_exc()}")

    def callback_new_report(self, data):
        """
        Process new liability reports to filter the one for the previously created liability.

        :param data: Report data.

        """
        if data[1]["index"] == self.index:
            report_content = ipfs_get_content(cid=ipfs_32_bytes_to_qm_hash(data[1]["payload"]["hash"]))
            logger.info(f"Robot has reported the liability with: {report_content}")

    def subscribe_new_reports(self):
        """

        :return:
        """

        Subscriber(
            account=self.user_acc, subscribed_event=SubEvent.NewReport, subscription_handler=self.callback_new_report
        )

    def run(self):
        """
        Run MQTT subscriber and NewReport subscribers in parallel threads.

        """

        mqtt_subscriber = Thread(target=self.connect_to_mqtt)
        mqtt_subscriber.start()

        new_report_subscriber = Thread(target=self.subscribe_new_reports)
        new_report_subscriber.start()

    def create_liability(self, technics: str, economics: int, promisor: str, promisor_signature: str):
        """
        Create new Robot liability with provided promisor data.

        :param technics: Liability technics.
        :param economics: Liability economics.
        :param promisor: Liability promisor -> EV3 robot address.
        :param promisor_signature: Liability promisor signature -> EV3 task signature.

        :return: Liability index, transaction hash.

        """
        liability_manager = Liability(self.user_acc)
        promisee_signature = liability_manager.sign_liability(technics, economics)

        return liability_manager.create(
            technics_hash=technics,
            economics=economics,
            promisee=self.user_acc.get_address(),
            promisor=promisor,
            promisee_params_signature=promisee_signature,
            promisor_params_signature=promisor_signature,
        )


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    ev3_client: EV3Client = EV3Client()
    ev3_client.run()

    time.sleep(2)

    task: dict = dict(
        addr=ev3_client.user_acc.get_address(), route=[[50, 50, 3], [20, 0, 2], [100, 100, 1]], price=15400001
    )

    while True:
        input()
        ev3_client.publish_offer(str(task))

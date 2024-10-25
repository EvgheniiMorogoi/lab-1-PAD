import socket
import xml.etree.ElementTree as ET

class Subscriber:
    def __init__(self, broker_ip, broker_port, topic):
        self.broker_ip = broker_ip
        self.broker_port = broker_port
        self.topic = topic

    def subscribe(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.broker_ip, self.broker_port))
            print(f"Subscribed to topic {self.topic}")

            # Trimitere mesaj de înregistrare
            registration_message = ET.Element('subscription')
            topic_element = ET.SubElement(registration_message, 'topic')
            topic_element.text = self.topic
            s.sendall(ET.tostring(registration_message, encoding='utf-8'))

            while True:
                data = s.recv(1024)
                if data:
                    message = self.parse_xml_message(data)
                    print(f"Received message on topic {self.topic}: {message}")

    def parse_xml_message(self, xml_data):
        try:
            root = ET.fromstring(xml_data.decode('utf-8'))
            topic = root.find('topic').text
            data = {}
            for child in root.find('data'):
                data[child.tag] = child.text
            return {'topic': topic, 'data': data}
        except Exception as e:
            print("Failed to parse XML:", e)
            return None

if __name__ == "__main__":
    subscriber = Subscriber('127.0.0.1', 65432, 'Second Message')
    subscriber.subscribe()

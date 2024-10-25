import socket
import xml.etree.ElementTree as ET

class Publisher:
    def __init__(self, broker_ip, broker_port):
        self.broker_ip = broker_ip
        self.broker_port = broker_port

    def create_xml_message(self, topic, data):
        # Cream elementul principal
        message = ET.Element('message')

        # Adăugăm subelementul topic
        topic_element = ET.SubElement(message, 'topic')
        topic_element.text = topic

        # Adăugăm subelementul data
        data_element = ET.SubElement(message, 'data')
        for key, value in data.items():
            element = ET.SubElement(data_element, key)
            element.text = str(value)

        # Convertim structura XML într-un string
        return ET.tostring(message, encoding='utf-8')

    def send_message(self, message):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.broker_ip, self.broker_port))
            s.sendall(message)
            print("Message sent to broker")

if __name__ == "__main__":
    publisher = Publisher('127.0.0.1', 65432)

    # Exemplar mesaj cu topicul "AddressChanged"
    message = publisher.create_xml_message('Second Message', {'user_id': 123, 'new_address': '123 Main St'})
    publisher.send_message(message)

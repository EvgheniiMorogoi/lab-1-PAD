import socket
import xml.etree.ElementTree as ET
import threading

class Broker:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.subscribers = {}

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            print(f"Broker started on {self.host}:{self.port}")

            while True:
                conn, addr = s.accept()
                threading.Thread(target=self.handle_connection, args=(conn, addr)).start()

    def handle_connection(self, conn, addr):
        print(f"Connected by {addr}")
        try:
            data = conn.recv(1024)
            if data:
                message_type, message = self.parse_message(data)
                if message_type == 'subscription':
                    topic = message['topic']
                    print(f"Subscriber registered for topic: {topic}")
                    self.register_subscriber(topic, conn)  # Leave the connection open for subscribers
                elif message_type == 'message':
                    print("Received message:", message)
                    self.route_message(message)
        except Exception as e:
            print(f"Error handling connection: {e}")
        # Nu mai închidem conexiunea aici, pentru a permite folosirea ei mai târziu.

    def parse_message(self, xml_data):
        try:
            root = ET.fromstring(xml_data.decode('utf-8'))
            if root.tag == 'subscription':
                topic = root.find('topic').text
                return 'subscription', {'topic': topic}
            elif root.tag == 'message':
                topic = root.find('topic').text
                data = {}
                data_element = root.find('data')
                if data_element is not None:
                    for child in data_element:
                        data[child.tag] = child.text
                return 'message', {'topic': topic, 'data': data}
        except Exception as e:
            print("Failed to parse XML:", e)
            return None, None

    def route_message(self, message):
        topic = message['topic']
        if topic in self.subscribers:
            for subscriber_conn in self.subscribers[topic]:
                try:
                    # Nu mai închidem conexiunea, trimitem mesajul în conexiunea deschisă
                    subscriber_conn.sendall(ET.tostring(self.create_xml_message(message)))
                    print(f"Message routed to subscriber for topic {topic}")
                except Exception as e:
                    print("Error sending message to subscriber:", e)

    def create_xml_message(self, message):
        # Refacem mesajul XML pentru a fi trimis la Subscriber
        message_element = ET.Element('message')

        topic_element = ET.SubElement(message_element, 'topic')
        topic_element.text = message['topic']

        data_element = ET.SubElement(message_element, 'data')
        for key, value in message['data'].items():
            element = ET.SubElement(data_element, key)
            element.text = str(value)

        return message_element

    def register_subscriber(self, topic, subscriber_conn):
        if topic not in self.subscribers:
            self.subscribers[topic] = []
        self.subscribers[topic].append(subscriber_conn)
        print(f"Subscriber added to topic {topic}")

if __name__ == "__main__":
    broker = Broker('127.0.0.1', 65432)
    threading.Thread(target=broker.start).start()

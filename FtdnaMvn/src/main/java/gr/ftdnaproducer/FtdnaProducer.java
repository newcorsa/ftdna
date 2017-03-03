package gr.ftdnaproducer;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

public class FtdnaProducer {

	// URL of the JMS server. DEFAULT_BROKER_URL will just mean
	// that JMS server is on localhost
	private static String url = ActiveMQConnection.DEFAULT_BROKER_URL;
	// default broker URL is : tcp://localhost:61616"

	private static String subject = "ftdna_first"; //Queue Name
	// You can create any/many queue names as per your requirement.

	public static void main(String[] args) throws JMSException {

		System.out.println("---start---");
		
		try {
			sendMessage( "Test message G.R. " + System.currentTimeMillis() );

			String result1 = receiveMessage();
			String result2 = receiveMessage();
			
		} catch(JMSException e) {
			System.out.println("EROOR: " + e.getMessage() );
		}
		
		System.out.println("---stop---");
	}
	
	public static void sendMessage( String msgBody ) throws JMSException {
		
		System.out.println("Sending...");

		// Getting JMS connection from the server and starting it
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
		Connection connection = connectionFactory.createConnection();
		connection.start();
		
		// JMS messages are sent and received using a Session. We will
		// create here a non-transactional session object. If you want
		// to use transactions you should set the first parameter to 'true'
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		
		// Destination represents here our queue 'VALLYSOFTQ' on the
		// JMS server. You don't have to do anything special on the
		// server to create it, it will be created automatically.
		Destination destination = session.createQueue(subject);
		
		// MessageProducer is used for sending messages (as opposed
		// to MessageConsumer which is used for receiving them)
		MessageProducer producer = session.createProducer(destination);
		
		// We will send a small text message saying 'Hello' in Japanese
		TextMessage message = session.createTextMessage( msgBody );
		
		// Here we are sending the message!
		producer.send(message);
		
		System.out.println("Sent: '" + message.getText() + "'");

		connection.close();		
	}
	
	public static String receiveMessage() throws JMSException {
		
		System.out.println("Receiving...");
		
		String result = "";
		
		// Getting JMS connection from the server
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
		Connection connection = connectionFactory.createConnection();
		connection.start();

		// Creating session for seding messages
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		// Getting the queue 'VALLYSOFTQ'
		Destination destination = session.createQueue(subject);

		// MessageConsumer is used for receiving (consuming) messages
		MessageConsumer consumer = session.createConsumer(destination);

		// Here we receive the message.
		// By default this call is blocking, which means it will wait
		// for a message to arrive on the queue.
		Message message = consumer.receive();

		// There are many types of Message and TextMessage
		// is just one of them. Producer sent us a TextMessage
		// so we must cast to it to get access to its .getText()
		// method.
		if (message instanceof TextMessage) {
			TextMessage textMessage = (TextMessage) message;
			System.out.println("Received: '" + textMessage.getText()	+ "'");
		
			result = textMessage.getText();
		}
		connection.close();
		
		return result;
	}
	
}

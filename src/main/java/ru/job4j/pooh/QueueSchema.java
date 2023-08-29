package ru.job4j.pooh;

import java.util.concurrent.*;

public class QueueSchema implements Schema {
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Receiver>> receivers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, BlockingQueue<String>> data = new ConcurrentHashMap<>();
    private final Condition condition = new Condition();

    @Override
    public void addReceiver(Receiver receiver) {
        receivers.putIfAbsent(receiver.name(), new CopyOnWriteArrayList<>());
        receivers.get(receiver.name()).add(receiver);
        condition.on();
    }

    @Override
    public void publish(Message message) {
        data.putIfAbsent(message.name(), new LinkedBlockingQueue<>());
        data.get(message.name()).add(message.text());
        condition.on();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            for (String topic : data.keySet()) {
                var textQueue = data.get(topic);
                var topicReceivers = receivers.get(topic);
                if (textQueue == null || topicReceivers == null) {
                    continue;
                }
                while (!textQueue.isEmpty()) {
                    for (Receiver receiver : topicReceivers) {
                        if (!textQueue.isEmpty()) {
                            receiver.receive(textQueue.poll());
                        }
                    }
                }
            }
            try {
                condition.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

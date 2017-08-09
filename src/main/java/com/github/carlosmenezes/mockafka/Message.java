package com.github.carlosmenezes.mockafka;

public class Message<K, V> {

    private final K key;
    private final V value;

    public Message(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Message<?, ?> message = (Message<?, ?>) o;

        if (!key.equals(message.key)) return false;
        return value.equals(message.value);
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }
}

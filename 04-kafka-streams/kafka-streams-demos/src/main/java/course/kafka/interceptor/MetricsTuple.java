package course.kafka.interceptor;

import lombok.Value;

import java.util.concurrent.atomic.AtomicLong;

@Value
public class MetricsTuple {
    public AtomicLong sent = new AtomicLong();
    public AtomicLong acknowledged = new AtomicLong();
    public AtomicLong errors = new AtomicLong();

    public long getSentValue() {
        return sent.get();
    }

    public long getAcknowledgedValue() {
        return acknowledged.get();
    }

    public long getErrorsValue() {
        return errors.get();
    }

    public boolean isNotZero() {
        return sent.get() != 0 || acknowledged.get() != 0 || errors.get() != 0;
    }

    public void setZero() {
        sent.set(0);
        acknowledged.set(0);
        errors.set(0);
    }
}

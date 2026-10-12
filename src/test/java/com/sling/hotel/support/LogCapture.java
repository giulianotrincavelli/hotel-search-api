package com.sling.hotel.support;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public final class LogCapture implements AutoCloseable {

    private final Logger logger;
    private final Level previousLevel;
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>() {
        @Override
        protected void append(ILoggingEvent event) {
            event.prepareForDeferredProcessing();
            super.append(event);
        }
    };

    public LogCapture(String loggerName) {
        logger = (Logger) LoggerFactory.getLogger(loggerName);
        previousLevel = logger.getLevel();
        logger.setLevel(Level.DEBUG);
        appender.list = new CopyOnWriteArrayList<>();
        appender.start();
        logger.addAppender(appender);
    }

    public List<ILoggingEvent> events() {
        return List.copyOf(appender.list);
    }

    @Override
    public void close() {
        logger.detachAppender(appender);
        logger.setLevel(previousLevel);
        appender.stop();
    }
}

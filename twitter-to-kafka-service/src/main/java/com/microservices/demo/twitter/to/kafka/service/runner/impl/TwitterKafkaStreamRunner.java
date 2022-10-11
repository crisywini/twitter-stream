package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import com.microservices.demo.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

@Component
@RequiredArgsConstructor
@Slf4j
public class TwitterKafkaStreamRunner implements StreamRunner {

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private TwitterStream twitterStream;

    @Override
    public void start() throws TwitterException {
        // We create the stream
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(twitterKafkaStatusListener);
        addFilterToStream(twitterStream);
    }

    @PreDestroy
    public void shutdown() {
        Optional.ofNullable(twitterStream).ifPresent(TwitterStream::shutdown);
    }

    private void addFilterToStream(TwitterStream twitterStream) {
        // Our keywords will do the "filter" part of the streaming process
        String[] keyWords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keyWords);
        // add the filter to the stream
        twitterStream.filter(filterQuery);
        log.info("Started filtering twitter stream for the keywords {}", keyWords);
    }

}

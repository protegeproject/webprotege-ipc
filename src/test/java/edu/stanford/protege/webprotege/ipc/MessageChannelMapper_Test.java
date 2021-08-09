package edu.stanford.protege.webprotege.ipc;

import edu.stanford.protege.webprotege.common.Request;
import edu.stanford.protege.webprotege.common.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2021-08-03
 */
@SpringBootTest
public class MessageChannelMapper_Test {

    private static String channelName = "ExpectedChannelName";

    @Autowired
    private MessageChannelMapper messageChannelMapper;

    @Value("${spring.application.name}")
    private String applicationName;

    @Mock
    private Request<? extends Response> request;

    @BeforeEach
    void setUp() {
        when(request.getChannel()).thenReturn(channelName);
    }

    @Test
    void shouldMapChannel() {
        var cn = messageChannelMapper.getChannelName(request);
        assertThat(cn).isEqualTo(channelName);
    }

    @Test
    void shouldMapReplyChannel() {
        var cn = messageChannelMapper.getReplyChannelName(request);
        assertThat(cn).contains(channelName);
        assertThat(cn).containsIgnoringCase("replies");
        assertThat(cn).contains(applicationName);
    }
}

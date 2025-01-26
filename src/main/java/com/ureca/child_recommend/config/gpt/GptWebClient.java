package com.ureca.child_recommend.config.gpt;

import com.ureca.child_recommend.contents.presentation.dto.GptDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Slf4j
public class GptWebClient {
    private WebClient webClient;
    private String recommendModel;

    public GptWebClient(String gptKey, String gptUrl, String recommendModel) {
        this.webClient = WebClient.builder()
                .defaultHeaders(httpHeaders -> {
                    httpHeaders.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE );
                    httpHeaders.add(HttpHeaders.AUTHORIZATION, "Bearer " + gptKey);
                })
                .baseUrl(gptUrl)
                .build();
        this.recommendModel = recommendModel;
    }

    public GptDto.Request of(int length) {
        return new GptDto.Request(recommendModel, length);
    }
    public Mono<GptDto.Response> assistantRes(GptDto.Request request){
        return webClient.post()
                .bodyValue(request)
                .retrieve()
                .bodyToMono(GptDto.Response.class);
    }
}

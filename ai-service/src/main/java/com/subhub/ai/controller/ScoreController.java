
package com.subhub.ai.controller;
import com.subhub.ai.dto.*; import com.subhub.ai.service.ScoreService;
import org.springframework.web.bind.annotation.*; import reactor.core.publisher.Mono; import reactor.core.scheduler.Schedulers;
@RestController @RequestMapping("/api/v1/churn")
public class ScoreController {
  private final ScoreService scoring;
  public ScoreController(ScoreService s){ this.scoring=s; }
  @PostMapping("/score") public Mono<SimpleScoreRes> simpleScore(@RequestBody ScoreReq req){
    return Mono.fromCallable(() -> scoring.simpleScore(req.userId(), req.prodId())).subscribeOn(Schedulers.boundedElastic());
  }
  @PostMapping("/score/detail") public Mono<ScoreRes> detailScore(@RequestBody ScoreReq req){
    return Mono.fromCallable(() -> scoring.detailScore(req.userId(), req.prodId())).subscribeOn(Schedulers.boundedElastic());
  }
}

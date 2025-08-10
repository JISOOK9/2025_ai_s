
package com.subhub.ai.controller;
import com.subhub.ai.dto.*; import com.subhub.ai.service.ScoringService;
import org.springframework.web.bind.annotation.*; import reactor.core.publisher.Mono; import reactor.core.scheduler.Schedulers;
@RestController @RequestMapping("/api/v1/churn")
public class ScoreController {
  private final ScoringService scoring;
  public ScoreController(ScoringService s){ this.scoring=s; }
  @PostMapping("/score") public Mono<ScoreRes> score(@RequestBody ScoreReq req){
    return Mono.fromCallable(() -> scoring.score(req.userId(), req.prodId())).subscribeOn(Schedulers.boundedElastic());
  }
}

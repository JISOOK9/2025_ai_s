
package com.subhub.ai.service;
import ai.onnxruntime.*; import com.subhub.ai.dto.*; import org.springframework.beans.factory.annotation.Value; import org.springframework.stereotype.Service;
import java.util.*; @Service public class ScoringService {
  private final FeatureAssembler features; public ScoringService(FeatureAssembler f){this.features=f;}
  @Value("${model.path:/tmp/model.onnx}") private String modelPath; private OrtEnvironment env; private OrtSession session;
  @jakarta.annotation.PostConstruct public void init() throws Exception { env=OrtEnvironment.getEnvironment(); session=env.createSession(modelPath, new OrtSession.SessionOptions()); }
  public ScoreRes score(java.util.UUID userId, String prodId) throws Exception {
    double[] f = features.assemble(userId, prodId); float[] ff=new float[f.length]; for(int i=0;i<f.length;i++) ff[i]=(float)f[i];
    try(OnnxTensor in=OnnxTensor.createTensor(env, new float[][]{ff}); OrtSession.Result out=session.run(Map.of(session.getInputNames().iterator().next(), in))) {
      float[][] v=(float[][])out.get(0).getValue(); double score=v[0][0]; String risk = score>=0.8? "high": score>=0.5? "med":"low";

      Factor top=null; // determine top factor by simple heuristics
      if(f.length>0 && f[0]>3) top=new Factor("fail_cnt_14d", f[0]);
      else if(f.length>15 && f[15]>0) top=new Factor("cancel_page_visit_14d", f[15]);

      String action; String reason;
      if("high".equals(risk)){
        if(top!=null && "fail_cnt_14d".equals(top.name())){ action="offer_coupon"; reason="Recent payment failures"; }
        else if(top!=null && "cancel_page_visit_14d".equals(top.name())){ action="show_retention_offer"; reason="Cancellation intent detected"; }
        else { action="reach_out"; reason="High churn risk"; }
      }else if("med".equals(risk)){ action="monitor"; reason="Medium churn risk"; }
      else { action="none"; reason="Low churn risk"; }

      Recommendation rec=new Recommendation(action, reason, Map.of("userId", userId, "prodId", prodId));
      java.util.List<Factor> factors=top==null? java.util.List.of(): java.util.List.of(top);
      return new ScoreRes(java.util.UUID.randomUUID().toString(), "churn_v1", score, risk, factors, rec);
    }
  }
}

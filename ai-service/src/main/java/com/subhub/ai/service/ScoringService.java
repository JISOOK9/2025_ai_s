package com.subhub.ai.service;

import ai.onnxruntime.*;
import com.subhub.ai.dto.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class ScoringService {

  private static final int TOP_N = 3;

  private static final String[] FEATURE_NAMES = {
      "fail_cnt_14d", "succ_cnt_14d", "coupon_amt_14d", "refund_amt_14d", "avg_amt_14d",
      "fail_cnt_30d", "succ_cnt_30d", "coupon_amt_30d", "refund_amt_30d", "avg_amt_30d",
      "switch_cnt_14d", "switch_cnt_30d", "downgraded_30d",
      "cancel_keyword_search_14d", "faq_cancel_views_14d", "cancel_page_visit_14d"
  };

  private final FeatureAssembler features;

  public ScoringService(FeatureAssembler f) {
    this.features = f;
  }

  @Value("${model.path:/tmp/model.onnx}")
  private String modelPath;

  private OrtEnvironment env;
  private OrtSession session;

  @jakarta.annotation.PostConstruct
  public void init() throws Exception {
    env = OrtEnvironment.getEnvironment();
    session = env.createSession(modelPath, new OrtSession.SessionOptions());
  }

  public ScoreRes score(java.util.UUID userId, String prodId) throws Exception {
    double[] f = features.assemble(userId, prodId);
    float[] ff = new float[f.length];
    for (int i = 0; i < f.length; i++) {
      ff[i] = (float) f[i];
    }

    try (OnnxTensor in = OnnxTensor.createTensor(env, new float[][]{ff});
         OrtSession.Result out = session.run(Map.of(session.getInputNames().iterator().next(), in))) {
      float[][] v = (float[][]) out.get(0).getValue();
      double score = v[0][0];

      List<Factor> topFactors = Collections.emptyList();
      if (out.size() > 1) {
        float[][] contribRaw = (float[][]) out.get(1).getValue();
        int offset = contribRaw[0].length == FEATURE_NAMES.length ? 0 : 1;
        List<Factor> factors = new ArrayList<>();
        for (int i = 0; i < FEATURE_NAMES.length && i + offset < contribRaw[0].length; i++) {
          factors.add(new Factor(FEATURE_NAMES[i], contribRaw[0][i + offset]));
        }
        factors.sort((a, b) -> Double.compare(Math.abs(b.contribution()), Math.abs(a.contribution())));
        if (factors.size() > TOP_N) {
          factors = new ArrayList<>(factors.subList(0, TOP_N));
        }
        topFactors = factors;
      }

      String risk = score >= 0.8 ? "high" : score >= 0.5 ? "med" : "low";
      return new ScoreRes(java.util.UUID.randomUUID().toString(), "churn_v1", score, risk, topFactors, null);
    }
  }
}


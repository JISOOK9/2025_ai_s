
package com.subhub.ai.dto;
import java.util.*; public record ScoreReq(UUID userId, String prodId) {}
public record Factor(String name, double contribution) {}
public record Recommendation(String action, String reason, Map<String,Object> params) {}
public record ScoreRes(String requestId, String modelVersion, double score, String riskLevel, List<Factor> topFactors, Recommendation recommendation) {}
public record SimpleScoreRes(String requestId, String modelVersion, double score, String riskLevel) {}

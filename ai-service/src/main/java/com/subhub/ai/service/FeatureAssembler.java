
package com.subhub.ai.service;
import org.springframework.jdbc.core.JdbcTemplate; import org.springframework.stereotype.Component; import java.util.UUID;
@Component public class FeatureAssembler {
  private final JdbcTemplate jdbc; public FeatureAssembler(JdbcTemplate j){this.jdbc=j;}
  public double[] assemble(UUID userId, String prodId){
    String sql = "SELECT fail_cnt_14d, succ_cnt_14d, coupon_amt_14d, refund_amt_14d, avg_amt_14d,"+
                 " fail_cnt_30d, succ_cnt_30d, coupon_amt_30d, refund_amt_30d, avg_amt_30d,"+
                 " switch_cnt_14d, switch_cnt_30d, downgraded_30d,"+
                 " cancel_keyword_search_14d, faq_cancel_views_14d, cancel_page_visit_14d"+
                 " FROM churn_feature_mv WHERE user_id=? AND prodId=?";
    return jdbc.query(sql, rs -> {
      if (!rs.next()) return new double[16]; double[] f=new double[16]; for(int i=0;i<16;i++) f[i]=rs.getDouble(i+1); return f;
    }, userId, prodId);
  }
}

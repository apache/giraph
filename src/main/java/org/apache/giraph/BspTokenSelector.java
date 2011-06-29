package org.apache.giraph;

import java.util.Collection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;

/**
  * Look through tokens to find the first job token that matches the service
  * and return it.
  */
public class BspTokenSelector implements TokenSelector<JobTokenIdentifier> {

    @SuppressWarnings("unchecked")
    @Override
    public Token<JobTokenIdentifier> selectToken(Text service,
            Collection<Token<? extends TokenIdentifier>> tokens) {
        if (service == null) {
            return null;
        }
        Text KIND_NAME = new Text("mapreduce.job");
        for (Token<? extends TokenIdentifier> token : tokens) {
            if (KIND_NAME.equals(token.getKind())) {
                return (Token<JobTokenIdentifier>) token;
            }
        }
        return null;
    }
}


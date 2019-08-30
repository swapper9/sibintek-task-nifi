package com.job.applicants.aptitude.test;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.*;

public class DeduplicatorProcessor extends AbstractProcessor {
  public static final Relationship SUCCESS = new Relationship.Builder()
    .name("SUCCESS")
    .description("Success relationship")
    .build();

  public static final Relationship FAILURE = new Relationship.Builder()
    .name("FAILURE")
    .description("FAILURE relationship")
    .build();

  public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(
    new HashSet<>(Arrays.asList(SUCCESS, FAILURE)));

  @Override
  public Set<Relationship> getRelationships() {
    return RELATIONSHIPS;
  }

  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    FlowFile flowFile = session.get();
    if (flowFile == null) return;
    List<String> deduplicatedStrings = new LinkedList<>();
    InputCache cache = new InputCache();
    Set<String> dictionary = cache.load();

    session.read(flowFile, (InputStream inputStream) -> {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
        String line = reader.readLine();
        while (line != null) {
          if (!line.isEmpty() && dictionary.add(line)) {
            deduplicatedStrings.add(line);
          }
          line = reader.readLine();
        }
      }
      cache.save(dictionary);
    });

    if (deduplicatedStrings.size() <= 0) {
      session.remove(flowFile);
    } else {
      flowFile = session.write(flowFile, (OutputStream out) -> out.write(String.join("\n", deduplicatedStrings).getBytes()));
      session.transfer(flowFile, SUCCESS);
    }
  }
}
package com.job.applicants.aptitude.test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DeduplicatorProcessorTest {

  private TestRunner testRunner = TestRunners.newTestRunner(DeduplicatorProcessor.class);

  @Test
  @DisplayName("Processor should remove duplicates for new batch")
  void batchDeduplicateTest() {
    clearCache();
    byte[] testData = "TAG1;1561025065;0.1;3\nTAG1;1561025065;0.1;3\nTAG2;1561025998;0.3;3".getBytes();
    testRunner.enqueue(testData);
    testRunner.run();

    List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(DeduplicatorProcessor.SUCCESS);
    List<String> result = flowFiles.stream()
      .map(a -> new String(testRunner.getContentAsByteArray(a)))
      .map(a -> new LinkedList<>(Arrays.asList(a.split("\n"))))
      .flatMap(List::stream)
      .collect(Collectors.toList());
    assertEquals(result.size(),2);
  }

  @Test
  @DisplayName("Processor should remove duplicates for all new batches")
  void newBatchDeduplicateTest() {
    clearBatchCache();
    byte[] testData = "TAG2;1561025998;0.3;3\nTAG3;1561025998;0.9;2".getBytes();
    testRunner.enqueue(testData);
    testRunner.run();

    List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(DeduplicatorProcessor.SUCCESS);
    List<String> result = flowFiles.stream()
      .map(a -> new String(testRunner.getContentAsByteArray(a)))
      .map(a -> new LinkedList<>(Arrays.asList(a.split("\n"))))
      .flatMap(List::stream)
      .collect(Collectors.toList());
    assertEquals(result.size(), 1);
  }

  void clearBatchCache() {
    try {
      Path from = Paths.get(System.getProperty("user.dir"), "input_batch_data.dat");
      Path to = Paths.get(System.getProperty("user.dir"), "input_data.dat");
      Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException ex) {
      Logger.getLogger(DeduplicatorProcessorTest.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  void clearCache() {
    try {
      Files.deleteIfExists(Paths.get(System.getProperty("user.dir"), "input_data.dat"));
    } catch (IOException ex) {
      System.out.println(ex);
    }
  }

}
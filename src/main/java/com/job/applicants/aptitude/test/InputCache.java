package com.job.applicants.aptitude.test;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class InputCache {

  private static Path path = Paths.get(System.getProperty("user.dir"), "input_data.dat");

  public void save(Set<String> data) {
    try {
      try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path.toFile())))) {
        for (String line : data) {
          bw.write(line);
          bw.newLine();
        }
        bw.flush();
      }
    } catch (IOException exc) {
      System.out.println(exc);
    }
  }

  public Set<String> load() {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path.toFile())))) {
      return reader.lines().collect(Collectors.toSet());
    } catch (IOException exc) {
      System.out.println(exc);
      return new HashSet<>();
    }
  }
}

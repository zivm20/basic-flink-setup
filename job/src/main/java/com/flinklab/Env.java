package com.flinklab;

import java.util.Locale;
import java.util.Optional;

/** Tiny helper for reading job configuration from environment variables. */
public final class Env {
  private Env() {}

  public static String require(String key) {
    String value = System.getenv(key);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Missing required env var: " + key);
    }
    return value.trim();
  }

  public static String get(String key, String defaultValue) {
    String value = System.getenv(key);
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    return value.trim();
  }

  public static int getInt(String key, int defaultValue) {
    return Integer.parseInt(get(key, Integer.toString(defaultValue)));
  }

  public static long getLong(String key, long defaultValue) {
    return Long.parseLong(get(key, Long.toString(defaultValue)));
  }

  public static boolean getBool(String key, boolean defaultValue) {
    return Optional.ofNullable(System.getenv(key))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .map(s -> s.equalsIgnoreCase("true") || s.equals("1") || s.equalsIgnoreCase("yes"))
        .orElse(defaultValue);
  }

  public static String upper(String value) {
    return value.toUpperCase(Locale.ROOT);
  }
}

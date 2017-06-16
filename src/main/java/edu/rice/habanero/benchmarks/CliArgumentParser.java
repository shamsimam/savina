package edu.rice.habanero.benchmarks;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

/**
 * @author <a href="https://github.com/aufgang001/">Sebastian Woelke</a> (aufgang001@posteo.de)
 */
public class CliArgumentParser {

    private Map<String, String> cliArgs = new HashMap<>();

    public CliArgumentParser(final String[] args) {
        int i = 0;
        while (i < args.length) {
            String value = new String();
            String key = args[i];
            if (key.charAt(0) == '-') {
                //it is an cli argument
                int idx = key.indexOf('=');
                if (idx != -1) {
                    // is a value assigned with '='
                    value = key.substring(idx + 1);
                    key = key.substring(0, idx);
                } else if ((i + 1) < args.length && args[i + 1].charAt(0) != '-') {
                    // has next argument und is it a value
                    value = args[i + 1];
                    i++;
                } else {
                    // value must be a boolean 
                    value = "true";
                }
          } else {
              System.out.println("Command line argument expected but got value: "  + key);
              System.exit(1);
          }
          cliArgs.put(key, value);
          i++;
        }
    }
  
    private interface Parser<V> {
        public V parse(String s);
    };

    private <V> V getValue(String[] key, V defaultValue, Parser<V> parser) {
        for (int i = 0; i < key.length; ++i) {
          String res = cliArgs.get(key[i]);
          if (res != null) {
              return parser.parse(res); 
          }
        }
        return defaultValue;
    }

    public int getIntValue(String[] key, int defaultValue) {
      Parser<Integer> parseInt = new Parser<Integer>() {
          public Integer parse(String s) {
              return Integer.parseInt(s);
          }
      };
      return getValue(key, defaultValue, parseInt);
    }

    public long getLongValue(String[] key, long defaultValue) {
      Parser<Long> parseLong = new Parser<Long>() {
          public Long parse(String s) {
              return Long.parseLong(s);
          }
      };
      return getValue(key, defaultValue, parseLong);
    }

    public double getDoubleValue(String[] key, double defaultValue) {
      Parser<Double> parseDouble = new Parser<Double>() {
          public Double parse(String s) {
              return Double.parseDouble(s);
          }
      };
      return getValue(key, defaultValue, parseDouble);
    }

    public boolean getBoolValue(String[] key, boolean defaultValue) {
      Parser<Boolean> parseBool = new Parser<Boolean>() {
          public Boolean parse(String s) {
              return Boolean.parseBoolean(s);
          }
      };
      return getValue(key, defaultValue, parseBool);
    }
}

package org.apache.atlas.util.lexoRank.system;


public class LexoNumeralSystem64 implements LexoNumeralSystem {

  private final char[] digits =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ^_abcdefghijklmnopqrstuvwxyz".toCharArray();

  public String getName() {
    return "Base64";
  }

  public int getBase() {
    return 64;
  }

  public char getPositiveChar() {
    return '+';
  }

  public char getNegativeChar() {
    return '-';
  }

  public char getRadixPointChar() {
    return ':';
  }

  public int toDigit(char ch) {
    if (ch >= '0' && ch <= '9') return ch - 48;
    if (ch >= 'A' && ch <= 'Z') return ch - 65 + 10;
    if (ch == '^') return 36;
    if (ch == '_') return 37;
    if (ch >= 'a' && ch <= 'z') return ch - 97 + 38;
    throw new IllegalArgumentException("Not valid digit: " + ch);
  }

  public char toChar(int digit) {
    return digits[digit];
  }
}

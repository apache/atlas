package org.apache.atlas.util.lexoRank.system;


public class LexoNumeralSystem36 implements LexoNumeralSystem {
  private final char[] digits = "0123456789abcdefghijklmnopqrstuvwxyz".toCharArray();

  public String getName() {
    return "Base36";
  }

  public int getBase() {
    return 36;
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
    if (ch >= 'a' && ch <= 'z') return ch - 97 + 10;
    throw new IllegalArgumentException("Not valid digit: " + ch);
  }

  public char toChar(int digit) {
    return digits[digit];
  }
}

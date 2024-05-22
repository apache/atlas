package org.apache.atlas.util.lexoRank.system;

public class LexoNumeralSystem10 implements LexoNumeralSystem {
  public String getName() {
    return "Base10";
  }

  public int getBase() {
    return 10;
  }

  public char getPositiveChar() {
    return '+';
  }

  public char getNegativeChar() {
    return '-';
  }

  public char getRadixPointChar() {
    return '.';
  }

  public int toDigit(char ch) {
    if (ch >= '0' && ch <= '9') return ch - 48;
    throw new IllegalArgumentException("Not valid digit: " + ch);
  }

  public char toChar(int digit) {
    return (char) (digit + 48);
  }
}

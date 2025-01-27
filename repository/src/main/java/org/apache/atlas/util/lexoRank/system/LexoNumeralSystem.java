package org.apache.atlas.util.lexoRank.system;

public interface LexoNumeralSystem {

  String getName();

  int getBase();

  char getPositiveChar();

  char getNegativeChar();

  char getRadixPointChar();

  int toDigit(char var1);

  char toChar(int var1);
}

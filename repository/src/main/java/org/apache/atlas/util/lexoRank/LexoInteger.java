package org.apache.atlas.util.lexoRank;


import org.apache.atlas.util.lexoRank.system.LexoNumeralSystem;

import java.util.Arrays;
import java.util.Objects;

public class LexoInteger implements Comparable<LexoInteger> {
  private static final int[] ZERO_MAG = {0};
  private static final int[] ONE_MAG = {1};
  private final int negativeSign = -1;
  private final int zeroSign = 0;
  private final int positiveSign = 1;
  private final int[] mag;
  private final int sign;
  private final LexoNumeralSystem sys;

  private LexoInteger(LexoNumeralSystem system, int sign, int[] mag) {
    sys = system;
    this.sign = sign;
    this.mag = mag;
  }

  private static int[] add(LexoNumeralSystem sys, int[] l, int[] r) {
    int estimatedSize = Math.max(l.length, r.length);
    int[] result = new int[estimatedSize];
    int carry = 0;

    for (int i = 0; i < estimatedSize; ++i) {
      int lNum = i < l.length ? l[i] : 0;
      int rNum = i < r.length ? r[i] : 0;
      int sum = lNum + rNum + carry;

      for (carry = 0; sum >= sys.getBase(); sum -= sys.getBase()) ++carry;

      result[i] = sum;
    }

    return extendWithCarry(result, carry);
  }

  private static int[] extendWithCarry(int[] mag, int carry) {
    int[] result = mag;
    if (carry > 0) {
      int[] extendedMag = new int[mag.length + 1];
      System.arraycopy(mag, 0, extendedMag, 0, mag.length);
      extendedMag[extendedMag.length - 1] = carry;
      result = extendedMag;
    }

    return result;
  }

  private static int[] subtract(LexoNumeralSystem sys, int[] l, int[] r) {
    int[] rComplement = complement(sys, r, l.length);
    int[] rSum = add(sys, l, rComplement);
    rSum[rSum.length - 1] = 0;
    return add(sys, rSum, ONE_MAG);
  }

  private static int[] multiply(LexoNumeralSystem sys, int[] l, int[] r) {
    int[] result = new int[l.length + r.length];

    for (int li = 0; li < l.length; ++li)
      for (int ri = 0; ri < r.length; ++ri) {
        int resultIndex = li + ri;

        for (result[resultIndex] += l[li] * r[ri];
            result[resultIndex] >= sys.getBase();
            result[resultIndex] -= sys.getBase()) ++result[resultIndex + 1];
      }

    return result;
  }

  private static int[] complement(LexoNumeralSystem sys, int[] mag, int digits) {
    if (digits <= 0) throw new IllegalArgumentException("Expected at least 1 digit");

    int[] nmag = new int[digits];

    Arrays.fill(nmag, sys.getBase() - 1);

    for (int i = 0; i < mag.length; ++i) nmag[i] = sys.getBase() - 1 - mag[i];

    return nmag;
  }

  private static int compare(int[] l, int[] r) {
    if (l.length < r.length) return -1;

    if (l.length > r.length) return 1;

    for (int i = l.length - 1; i >= 0; --i) {
      if (l[i] < r[i]) return -1;

      if (l[i] > r[i]) return 1;
    }

    return 0;
  }

  public static LexoInteger parse(String strFull, LexoNumeralSystem system) {
    String str = strFull;
    int sign = 1;
    if (strFull.indexOf(system.getPositiveChar()) == 0) {
      str = strFull.substring(1);
    } else if (strFull.indexOf(system.getNegativeChar()) == 0) {
      str = strFull.substring(1);
      sign = -1;
    }

    int[] mag = new int[str.length()];
    int strIndex = mag.length - 1;

    for (int magIndex = 0; strIndex >= 0; ++magIndex) {
      mag[magIndex] = system.toDigit(str.charAt(strIndex));
      --strIndex;
    }

    return make(system, sign, mag);
  }

  protected static LexoInteger zero(LexoNumeralSystem sys) {
    return new LexoInteger(sys, 0, ZERO_MAG);
  }

  protected static LexoInteger one(LexoNumeralSystem sys) {
    return make(sys, 1, ONE_MAG);
  }

  public static LexoInteger make(LexoNumeralSystem sys, int sign, int[] mag) {
    int actualLength;
    actualLength = mag.length;
    while (actualLength > 0 && mag[actualLength - 1] == 0) {
      --actualLength;
    }

    if (actualLength == 0) return zero(sys);

    if (actualLength == mag.length) return new LexoInteger(sys, sign, mag);

    int[] nmag = new int[actualLength];
    System.arraycopy(mag, 0, nmag, 0, actualLength);
    return new LexoInteger(sys, sign, nmag);
  }

  public LexoInteger add(LexoInteger other) {
    checkSystem(other);
    if (isZero()) return other;

    if (other.isZero()) return this;

    if (sign != other.sign) {
      LexoInteger pos;
      if (sign == -1) {
        pos = negate();
        LexoInteger val = pos.subtract(other);
        return val.negate();
      }

      pos = other.negate();
      return subtract(pos);
    }

    int[] result = add(sys, mag, other.mag);
    return make(sys, sign, result);
  }

  public LexoInteger subtract(LexoInteger other) {
    checkSystem(other);
    if (isZero()) return other.negate();

    if (other.isZero()) return this;

    if (sign != other.sign) {
      LexoInteger negate;
      if (sign == -1) {
        negate = negate();
        LexoInteger sum = negate.add(other);
        return sum.negate();
      }

      negate = other.negate();
      return add(negate);
    }

    int cmp = compare(mag, other.mag);
    if (cmp == 0) return zero(sys);

    return cmp < 0
        ? make(sys, sign == -1 ? 1 : -1, subtract(sys, other.mag, mag))
        : make(sys, sign == -1 ? -1 : 1, subtract(sys, mag, other.mag));
  }

  public LexoInteger multiply(LexoInteger other) {
    checkSystem(other);
    if (isZero()) return this;

    if (other.isZero()) return other;

    if (isOneish()) return sign == other.sign ? make(sys, 1, other.mag) : make(sys, -1, other.mag);

    if (other.isOneish()) return sign == other.sign ? make(sys, 1, mag) : make(sys, -1, mag);

    int[] newMag = multiply(sys, mag, other.mag);
    return sign == other.sign ? make(sys, 1, newMag) : make(sys, -1, newMag);
  }

  public LexoInteger negate() {
    return isZero() ? this : make(sys, sign == 1 ? -1 : 1, mag);
  }

  public LexoInteger shiftLeft() {
    return shiftLeft(1);
  }

  public LexoInteger shiftLeft(int times) {
    if (times == 0) return this;

    if (times < 0) return shiftRight(Math.abs(times));

    int[] nmag = new int[mag.length + times];
    System.arraycopy(mag, 0, nmag, times, mag.length);
    return make(sys, sign, nmag);
  }

  public LexoInteger shiftRight() {
    return shiftRight(1);
  }

  public LexoInteger shiftRight(int times) {
    if (mag.length - times <= 0) return zero(sys);

    int[] nmag = new int[mag.length - times];
    System.arraycopy(mag, times, nmag, 0, nmag.length);
    return make(sys, sign, nmag);
  }

  public LexoInteger complement() {
    return complement(mag.length);
  }

  private LexoInteger complement(int digits) {
    return make(sys, sign, complement(sys, mag, digits));
  }

  public boolean isZero() {
    return sign == 0 && mag.length == 1 && mag[0] == 0;
  }

  private boolean isOneish() {
    return mag.length == 1 && mag[0] == 1;
  }

  public boolean isOne() {
    return sign == 1 && mag.length == 1 && mag[0] == 1;
  }

  public int getMag(int index) {
    return mag[index];
  }

  public LexoNumeralSystem getSystem() {
    return sys;
  }

  private void checkSystem(LexoInteger other) {
    if (!sys.getName().equals(other.sys.getName()))
      throw new IllegalArgumentException("Expected numbers of same numeral sys");
  }

  public String format() {
    if (isZero()) return String.valueOf(sys.toChar(0));
    StringBuilder sb = new StringBuilder();
    for (int digit : mag) {
      sb.insert(0, sys.toChar(digit));
    }
    if (sign == -1) sb.setCharAt(0, sys.getNegativeChar());

    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LexoInteger that = (LexoInteger) o;
    return sign == that.sign && Arrays.equals(mag, that.mag) && Objects.equals(sys, that.sys);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(negativeSign, zeroSign, positiveSign, sign, sys);
    result = 31 * result + Arrays.hashCode(mag);
    return result;
  }

  @Override
  public String toString() {
    return format();
  }

  @Override
  public int compareTo(LexoInteger lexoInteger) {
    if (this.equals(lexoInteger)) return 0;
    if (null == lexoInteger) return 1;

    if (sign == -1) {
      if (lexoInteger.sign == -1) {
        int cmp = compare(mag, lexoInteger.mag);
        if (cmp == -1) return 1;
        return cmp == 1 ? -1 : 0;
      }

      return -1;
    }

    if (sign == 1) return lexoInteger.sign == 1 ? compare(mag, lexoInteger.mag) : 1;

    if (lexoInteger.sign == -1) return 1;

    return lexoInteger.sign == 1 ? -1 : 0;
  }
}

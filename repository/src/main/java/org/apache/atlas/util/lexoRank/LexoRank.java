package org.apache.atlas.util.lexoRank;




import org.apache.atlas.util.lexoRank.system.LexoNumeralSystem;
import org.apache.atlas.util.lexoRank.system.LexoNumeralSystem36;

import java.util.Objects;

public class LexoRank implements Comparable<LexoRank> {

  public static final LexoNumeralSystem NUMERAL_SYSTEM = new LexoNumeralSystem36();
  private static final LexoDecimal ZERO_DECIMAL = LexoDecimal.parse("0", NUMERAL_SYSTEM);
  private static final LexoDecimal ONE_DECIMAL = LexoDecimal.parse("1", NUMERAL_SYSTEM);
  private static final LexoDecimal EIGHT_DECIMAL = LexoDecimal.parse("8", NUMERAL_SYSTEM);
  private static final LexoDecimal MIN_DECIMAL = ZERO_DECIMAL;

  private static final LexoDecimal MAX_DECIMAL =
      LexoDecimal.parse("1000000", NUMERAL_SYSTEM).subtract(ONE_DECIMAL);

  private static final LexoDecimal MID_DECIMAL = between(MIN_DECIMAL, MAX_DECIMAL);
  private static final LexoDecimal INITIAL_MIN_DECIMAL =
      LexoDecimal.parse("100000", NUMERAL_SYSTEM);

  private static final LexoDecimal INITIAL_MAX_DECIMAL =
      LexoDecimal.parse(
          NUMERAL_SYSTEM.toChar(NUMERAL_SYSTEM.getBase() - 2) + "00000", NUMERAL_SYSTEM);

  private final String value;
  private final LexoRankBucket bucket;
  private final LexoDecimal decimal;

  private LexoRank(String value) {
    this.value = value;
    String[] parts = this.value.split("\\|");
    bucket = LexoRankBucket.from(parts[0]);
    decimal = LexoDecimal.parse(parts[1], NUMERAL_SYSTEM);
  }

  private LexoRank(LexoRankBucket bucket, LexoDecimal dec) {
    value = bucket.format() + "|" + formatDecimal(dec);
    this.bucket = bucket;
    decimal = dec;
  }

  public static LexoRank min() {
    return from(LexoRankBucket.BUCKET_0, MIN_DECIMAL);
  }

  public static LexoRank max() {
    return max(LexoRankBucket.BUCKET_0);
  }

  public static LexoRank middle() {
    LexoRank minLexoRank = min();
    return minLexoRank.between(max(minLexoRank.bucket));
  }

  public static LexoRank max(LexoRankBucket bucket) {
    return from(bucket, MAX_DECIMAL);
  }

  public static LexoRank initial(LexoRankBucket bucket) {
    return bucket == LexoRankBucket.BUCKET_0
        ? from(bucket, INITIAL_MIN_DECIMAL)
        : from(bucket, INITIAL_MAX_DECIMAL);
  }

  private static LexoDecimal between(LexoDecimal oLeft, LexoDecimal oRight) {
    if (oLeft.getSystem() != oRight.getSystem())
      throw new IllegalArgumentException("Expected same system");

    LexoDecimal left = oLeft;
    LexoDecimal right = oRight;
    LexoDecimal nLeft;
    if (oLeft.getScale() < oRight.getScale()) {
      nLeft = oRight.setScale(oLeft.getScale(), false);
      if (oLeft.compareTo(nLeft) >= 0) return middle(oLeft, oRight);

      right = nLeft;
    }

    if (oLeft.getScale() > right.getScale()) {
      nLeft = oLeft.setScale(right.getScale(), true);
      if (nLeft.compareTo(right) >= 0) return middle(oLeft, oRight);

      left = nLeft;
    }

    LexoDecimal nRight;
    for (int scale = left.getScale(); scale > 0; right = nRight) {
      int nScale1 = scale - 1;
      LexoDecimal nLeft1 = left.setScale(nScale1, true);
      nRight = right.setScale(nScale1, false);
      int cmp = nLeft1.compareTo(nRight);
      if (cmp == 0) return checkMid(oLeft, oRight, nLeft1);

      if (nLeft1.compareTo(nRight) > 0) break;

      scale = nScale1;
      left = nLeft1;
    }

    LexoDecimal mid = middle(oLeft, oRight, left, right);

    int nScale;
    for (int mScale = mid.getScale(); mScale > 0; mScale = nScale) {
      nScale = mScale - 1;
      LexoDecimal nMid = mid.setScale(nScale);
      if (oLeft.compareTo(nMid) >= 0 || nMid.compareTo(oRight) >= 0) break;

      mid = nMid;
    }

    return mid;
  }

  private static LexoDecimal middle(
          LexoDecimal lBound, LexoDecimal rBound, LexoDecimal left, LexoDecimal right) {
    LexoDecimal mid = middle(left, right);
    return checkMid(lBound, rBound, mid);
  }

  private static LexoDecimal checkMid(LexoDecimal lBound, LexoDecimal rBound, LexoDecimal mid) {
    if (lBound.compareTo(mid) >= 0) return middle(lBound, rBound);

    return mid.compareTo(rBound) >= 0 ? middle(lBound, rBound) : mid;
  }

  private static LexoDecimal middle(LexoDecimal left, LexoDecimal right) {
    LexoDecimal sum = left.add(right);
    LexoDecimal mid = sum.multiply(LexoDecimal.half(left.getSystem()));
    int scale = Math.max(left.getScale(), right.getScale());
    if (mid.getScale() > scale) {
      LexoDecimal roundDown = mid.setScale(scale, false);
      if (roundDown.compareTo(left) > 0) return roundDown;

      LexoDecimal roundUp = mid.setScale(scale, true);
      if (roundUp.compareTo(right) < 0) return roundUp;
    }

    return mid;
  }

  private static String formatDecimal(LexoDecimal dec) {
    String formatVal = dec.format();
    StringBuilder val = new StringBuilder(formatVal);
    int partialIndex = formatVal.indexOf(NUMERAL_SYSTEM.getRadixPointChar());
    char zero = NUMERAL_SYSTEM.toChar(0);
    if (partialIndex < 0) {
      partialIndex = formatVal.length();
      val.append(NUMERAL_SYSTEM.getRadixPointChar());
    }

    while (partialIndex < 6) {
      val.insert(0, zero);
      ++partialIndex;
    }

    // TODO CHECK LOGIC
    int valLength = val.length() - 1;
    while (val.charAt(valLength) == zero) {
      valLength = val.length() - 1;
    }

    return val.toString();
  }

  public static LexoRank parse(String str) {
    if (isNullOrWhiteSpace(str)) throw new IllegalArgumentException(str);
    return new LexoRank(str);
  }

  public static LexoRank from(LexoRankBucket bucket, LexoDecimal dec) {
    if (!dec.getSystem().getName().equals(NUMERAL_SYSTEM.getName()))
      throw new IllegalArgumentException("Expected different system");

    return new LexoRank(bucket, dec);
  }

  private static boolean isNullOrWhiteSpace(String string) {
    return string == null || string.equals(" ");
  }

  public LexoRankBucket getBucket() {
    return bucket;
  }

  public LexoDecimal getDecimal() {
    return decimal;
  }

  public int CompareTo(LexoRank other) {
    if (Objects.equals(this, other)) return 0;
    if (Objects.equals(null, other)) return 1;
    return value.compareTo(other.value);
  }

  public LexoRank genPrev() {
    if (isMax()) return new LexoRank(bucket, INITIAL_MAX_DECIMAL);

    LexoInteger floorInteger = decimal.floor();
    LexoDecimal floorDecimal = LexoDecimal.from(floorInteger);
    LexoDecimal nextDecimal = floorDecimal.subtract(EIGHT_DECIMAL);
    if (nextDecimal.compareTo(MIN_DECIMAL) <= 0) nextDecimal = between(MIN_DECIMAL, decimal);

    return new LexoRank(bucket, nextDecimal);
  }

  public LexoRank inNextBucket() {
    return from(bucket.next(), decimal);
  }

  public LexoRank inPrevBucket() {
    return from(bucket.prev(), decimal);
  }

  public boolean isMin() {
    return decimal.equals(MIN_DECIMAL);
  }

  public boolean isMax() {
    return decimal.equals(MAX_DECIMAL);
  }

  public String format() {
    return value;
  }

  public LexoRank genNext() {
    if (isMin()) return new LexoRank(bucket, INITIAL_MIN_DECIMAL);

    LexoInteger ceilInteger = decimal.ceil();
    LexoDecimal ceilDecimal = LexoDecimal.from(ceilInteger);
    LexoDecimal nextDecimal = ceilDecimal.add(EIGHT_DECIMAL);
    if (nextDecimal.compareTo(MAX_DECIMAL) >= 0) nextDecimal = between(decimal, MAX_DECIMAL);

    return new LexoRank(bucket, nextDecimal);
  }

  public LexoRank between(LexoRank other) {
    if (!bucket.equals(other.bucket))
      throw new IllegalArgumentException("Between works only within the same bucket");

    int cmp = decimal.compareTo(other.decimal);
    if (cmp > 0) return new LexoRank(bucket, between(other.decimal, decimal));
    if (cmp == 0)
      throw new IllegalArgumentException(
          "Try to rank between issues with same rank this="
              + this
              + " other="
              + other
              + " this.decimal="
              + decimal
              + " other.decimal="
              + other.decimal);
    return new LexoRank(bucket, between(decimal, other.decimal));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LexoRank lexoRank = (LexoRank) o;
    return Objects.equals(value, lexoRank.value)
        && Objects.equals(bucket, lexoRank.bucket)
        && Objects.equals(decimal, lexoRank.decimal);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, bucket, decimal);
  }

  @Override
  public String toString() {
    return format();
  }

  @Override
  public int compareTo(LexoRank lexoRank) {
    if (Objects.equals(this, lexoRank)) return 0;
    if (Objects.equals(null, lexoRank)) return 1;
    return value.compareTo(lexoRank.value);
  }
}

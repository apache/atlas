package org.apache.atlas.util.lexoRank;


import java.util.Objects;

public class LexoRankBucket {

  protected static final LexoRankBucket BUCKET_0 = new LexoRankBucket("0");
  protected static final LexoRankBucket BUCKET_1 = new LexoRankBucket("1");
  protected static final LexoRankBucket BUCKET_2 = new LexoRankBucket("2");

  private static final LexoRankBucket[] VALUES = {BUCKET_0, BUCKET_1, BUCKET_2};

  private final LexoInteger value;

  private LexoRankBucket(String val) {
    value = LexoInteger.parse(val, LexoRank.NUMERAL_SYSTEM);
  }

  public static LexoRankBucket resolve(int bucketId) {
    for (LexoRankBucket bucket : VALUES) {
      if (bucket.equals(from(String.valueOf(bucketId)))) return bucket;
    }

    throw new IllegalArgumentException("No bucket found with id " + bucketId);
  }

  public static LexoRankBucket from(String str) {
    LexoInteger val = LexoInteger.parse(str, LexoRank.NUMERAL_SYSTEM);

    for (LexoRankBucket bucket : VALUES) {
      if (bucket.value.equals(val)) return bucket;
    }

    throw new IllegalArgumentException("Unknown bucket: " + str);
  }

  public static LexoRankBucket min() {
    return VALUES[0];
  }

  public static LexoRankBucket max() {
    return VALUES[VALUES.length - 1];
  }

  public String format() {
    return value.format();
  }

  public LexoRankBucket next() {
    if (this == BUCKET_0) return BUCKET_1;

    if (this == BUCKET_1) return BUCKET_2;

    return this == BUCKET_2 ? BUCKET_0 : BUCKET_2;
  }

  public LexoRankBucket prev() {
    if (this == BUCKET_0) return BUCKET_2;

    if (this == BUCKET_1) return BUCKET_0;

    return this == BUCKET_2 ? BUCKET_1 : BUCKET_0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LexoRankBucket that = (LexoRankBucket) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public String toString() {
    return format();
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}

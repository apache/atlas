package org.apache.metadata.types;


public final class Multiplicity {

    public final int lower;
    public final int upper;
    public final boolean isUnique;

    private Multiplicity(int lower, int upper, boolean isUnique) {
        assert lower >= 0;
        assert upper >= 1;
        assert upper >= lower;
        this.lower = lower;
        this.upper = upper;
        this.isUnique = isUnique;
    }

    public boolean nullAllowed() {
        return lower == 0;
    }

    @Override
    public String toString() {
        return "Multiplicity{" +
                "lower=" + lower +
                ", upper=" + upper +
                ", isUnique=" + isUnique +
                '}';
    }

    public static final Multiplicity OPTIONAL = new Multiplicity(0, 1, false);
    public static final Multiplicity REQUIRED = new Multiplicity(1, 1, false);
    public static final Multiplicity COLLECTION = new Multiplicity(1, Integer.MAX_VALUE, false);
    public static final Multiplicity SET = new Multiplicity(1, Integer.MAX_VALUE, true);
}

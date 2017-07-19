package org.apache.lucene.search.facets;

import java.util.List;

public abstract class AbstractFacetField implements FacetField {

	public static class IntEntry implements FacetEntry<Integer, Integer> {
		int key;
		int count;
		public IntEntry(int key, int count) {
			this.key = key;
			this.count = count;
		}

		public Integer getKey() {
			return key;
		}

		public Integer getValue() {
			return count;
		}
	}

	public static class IntRange {
		public int start;
		public int end;
		public IntRange(int start, int end) {
			this.start = start;
			this.end = end;
		}
	}

	public static class IntRangeEntry implements FacetEntry<IntRange, Integer> {
		IntRange range;
		int count;

		public IntRangeEntry(int start, int end, int count) {
			this.range = new IntRange(start, end);
			this.count = count;
		}

		public IntRange getKey() {
			return range;
		}

		public Integer getValue() {
			return count;
		}
	}

	static class FloatEntry implements FacetEntry<Float, Integer> {
		float key;
		int count;
		public FloatEntry(float key, int count) {
			this.key = key;
			this.count = count;
		}

		public Float getKey() {
			return key;
		}

		public Integer getValue() {
			return count;
		}
	}

	public static class FloatRange {
		public float start;
		public float end;
		public FloatRange(float start, float end) {
			this.start = start;
			this.end = end;
		}
	}

	public static class FloatRangeEntry implements FacetEntry<FloatRange, Integer> {
		FloatRange range;
		int count;

		public FloatRangeEntry(float start, float end, int count) {
			this.range = new FloatRange(start, end);
			this.count = count;
		}

		public FloatRange getKey() {
			return range;
		}

		public Integer getValue() {
			return count;
		}
	}

	static class StringEntry implements FacetEntry<String, Integer> {
		String key;
		int count;
		public StringEntry(String key, int count) {
			this.key = key;
			this.count = count;
		}

		public String getKey() {
			return key;
		}

		public Integer getValue() {
			return count;
		}
	}

	// 只返回结果大于0的项
	public List<FacetEntry> getFacetResult() {
		return getFacetResult(1);
	}

	public List<FacetEntry> getFacetResult(int minCount) {
		throw new UnsupportedOperationException("This method was not implemented by sub class!");
	}	
}

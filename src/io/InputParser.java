package io;

import java.nio.ByteBuffer;
import quadIndex.Point;
import quadIndex.Rect;
import java.util.*;


public class InputParser {
	public static Rect getObjFromLine(String line) {
		String data = line;
		String[] strings = data.split(" ");
		if((strings.length & 1) == 1)
			return null;
		List<Double> nums = new ArrayList<Double>();
		for(int i = 0;i<strings.length;i++)
			nums.add(Double.parseDouble(strings[i]));
		Rect r = new Rect(nums.get(0),nums.get(1),nums.get(2),nums.get(3));
		return r;
	}
}

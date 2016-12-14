package com.bytro.firefly.sql;

import java.util.List;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableList;
import org.springframework.stereotype.Component;

/**
 * A basic list to filter column names we are interested in.
 *
 * Returns true if any of whiteList regexes is true.
 */
@Component
public class ColumnFilter implements Predicate<String> {
	private static List<String> whiteList = ImmutableList.of(
			"unit(\\d+)killed",
			"unit(\\d+)killedAI",
			"unit(\\d+)lost",
			"unit(\\d+)lostAI",
			"killedWithUnit(\\d+)",
			"killedWithUnit(\\d+)AI"
	);

	@Override
	public boolean test(String s) {
		return whiteList.stream().anyMatch(s::matches);
	}
}

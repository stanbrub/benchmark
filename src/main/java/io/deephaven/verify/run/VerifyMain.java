package io.deephaven.verify.run;

import java.util.*;

public class VerifyMain {
	static public void main(String[] args) {
		args = addDefaults(args);
		org.junit.platform.console.ConsoleLauncher.main(args);
	}
	
	static String[] addDefaults(String[] args) {
		var newArgs = new ArrayList<String>(Arrays.asList(args));
		addDefault(newArgs, "-n", "--include-classname=", "^(.*)$");
		addDefault(newArgs, "-p", "--select-package=", "io.deephaven.verify.tests");
		return newArgs.toArray(new String[newArgs.size()]);
	}
	
	static void addDefault(List<String> args, String shortSwitch, String longSwitch, String defaultValue) {
		if(foundShortSwitch(args, shortSwitch) || foundLongSwitch(args, longSwitch)) return;
		args.add(shortSwitch);
		args.add(defaultValue);
	}

	static boolean foundShortSwitch(List<String> args, String shortSwitch) {
		return (args.indexOf(shortSwitch) >= 0);
	}
	
	static boolean foundLongSwitch(List<String> args, String longSwitch) {
		return args.stream().anyMatch(a->a.startsWith(longSwitch));
	}

}

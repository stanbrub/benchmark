package io.deephaven.verify.run;

public class VerifyMain {
	static public void main(String[] args) {
		System.setProperty("verify.dir", System.getProperty("user.dir"));
		System.exit(new VerifyMain().main1(args));
	}

	public int main1(String...args) {
		if(args.length != 1) return printUsage();
		return 0;
	}
	
	private int printUsage() {
		System.out.println("Usage: VerifyMain <profile.uri>");
		return 1;
	}

}

/*
 * Created on 2004/8/13
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package third.party.util;

import java.io.PrintStream;

/**
 * @author Administrator
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class MessageLogger {
    public static PrintStream out = System.out;
    public static PrintStream err = System.err;
    
    public static void setErr(PrintStream err) {
    	MessageLogger.err = err;
    }
    public static void setOut(PrintStream out) {
    	MessageLogger.out = out;
    }
}

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
    public static String hostToStandart(String host) {
        host = host.trim();
        host = host.replaceAll("/+$", "");
        host = host.replaceAll("^/+", "");
        return host;
    }

    private static  Pattern regex = Pattern.compile("([(){}.?|^+])");
    public static String constStringToRegex(String str) {
        Matcher regexMatcher = regex.matcher(str);
        str = regexMatcher.replaceAll("\\\\" + "$1");
        return str;
    }
}

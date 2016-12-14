
public class Utils {
    public static String hostToStandart(String host) {
        host = host.trim();
        host = host.replaceAll("/+$", "");
        host = host.replaceAll("^/+", "");
        return host;
    }
}

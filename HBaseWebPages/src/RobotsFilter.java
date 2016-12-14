import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class RobotsFilter {
    public  void addRules(String newRobots) {
        for (String rule: newRobots.split("\n")) {
            if (rule.isEmpty()) {
                continue;
            }
            rules.add(ruleToRegex(rule.substring(10)));
        }
    }
    public boolean isDisallowed(String url) throws MalformedURLException {
        String path = getPath(url).trim();
        for (String rule: rules) {

            if (path.matches(rule)) {
                return true;
            }
        }
        return false;
    }
    private String getPath(String sUrl) throws MalformedURLException {
        URL url = new URL(sUrl);
        String protocol = url.getProtocol();
        String host = url.getHost();
        String ref = url.getRef();
        int port = url.getPort();
        String result = sUrl.replaceFirst(Utils.constStringToRegex(protocol + ":"), "");
        result = result.replaceFirst("^/+", "");
        result = result.replaceFirst(Utils.constStringToRegex(host), "");
        result = result.replaceFirst(Utils.constStringToRegex("#" + ref), "");
        if (port > 0) {
            result = result.replaceFirst(Utils.constStringToRegex(":" + Integer.toString(port)), "");
        }
        return result;
    }
    private String ruleToRegex(String rule) {
        rule = Utils.constStringToRegex(rule);
        rule = "^" + rule;
        if (!rule.endsWith("$")) {
            rule += "*";
        }
        rule = rule.replaceAll("\\*", "\\.\\*");
        return rule;
    }
    private List<String> rules = new ArrayList<>();
}

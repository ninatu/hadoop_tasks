import java.util.ArrayList;
import java.util.List;

/**
 * Created by nina on 01.12.16.
 */
public class RobotsFilter {
    public  void addRules(String newRobots) {
        for (String rule: newRobots.split("\n")) {
            if (rule.isEmpty()) {
                continue;
            }
            rules.add(toRegex(rule.substring(10)));
        }
    }

    public boolean isDisallowed(String url) {
        for (String rule: rules) {
            //System.out.println(rule + ":" + url);
            //System.out.flush();
            if (url.matches(rule)) {
                return true;
            }
        }
        return false;
    }
    private String toRegex(String rule) {
        rule = "^" + rule;
        if (!rule.endsWith("$")) {
            rule += "*";
        }
        rule = rule.replaceAll("\\*", "\\.\\*");
        return rule;
    }

    private List<String> rules = new ArrayList<>();
}

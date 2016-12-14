/**
 * Created by nina on 01.12.16.
 */
public class Test {
    public static void main(String[] args) {

        String rules = "Disallow: /users\n" +
                "Disallow: *.php$\n" +
                "Disallow: */cgi-bin/\n" +
                "Disallow: /very/secret.page.html$\n";

        RobotsFilter filter = new RobotsFilter();
        filter.addRules(rules);

        System.out.println("false:" + filter.isDisallowed("/users/jan"));
        System.out.println("true:" + "should be allowed since in the middle" + filter.isDisallowed("/subdir2/users/about.html"));

        System.out.println("false:" + filter.isDisallowed("/info.php"));
        System.out.println("true:" + "we disDisallowed only the endler" +  filter.isDisallowed("/info.php?user=123"));
        System.out.println("true:" + filter.isDisallowed("/info.pl"));

        System.out.println("false:" + filter.isDisallowed("/forum/cgi-bin/send?user=123"));
        System.out.println("false:" + filter.isDisallowed("/forum/cgi-bin/"));
        System.out.println("false:" + filter.isDisallowed("/cgi-bin/"));
        System.out.println("true:" + filter.isDisallowed("/scgi-bin/"));


        System.out.println("false:" + filter.isDisallowed("/very/secret.page.html"));
        System.out.println("true:" + "we disDisallowed only the whole match" + filter.isDisallowed("/the/very/secret.page.html"));
        System.out.println("true:" + "we disDisallowed only the whole match" + filter.isDisallowed("/very/secret.page.html?blah"));
        System.out.println("true:" + "we disDisallowed only the whole match" + filter.isDisallowed("/the/very/secret.page.html?blah"));
        String a = new String(new byte[0]);
        System.out.println("AAA" + a);


    }
}

package edu.isi.dig.elasticsearch.mapreduce.driver;

import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.codec.digest.DigestUtils;


public class DeduplicateEntities {

    private final static int PAGESIZE = 500;

    public static Optional<String> loadFile(Path p) {
        try {
            return Optional.of(new String(Files.readAllBytes(p), StandardCharsets.UTF_8));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public static Optional<String> extractIdFromPost(Element post) {
        Elements elements = post.select("h2");
        if (!elements.isEmpty()) {
            Elements a = elements.first().select("a");
            if (!a.isEmpty()) {
                return Optional.of(a.first().attr("href"));
            }
        }
        return Optional.empty();
    }

    public static String buildPostHash(String postId, String postBody) {
        return postId + "::" + DigestUtils.sha256Hex(postBody);
    }

    private final static String EMPTY = "";

    private static void preprocessEvolutionForums(String srcPath, String destPath) {

        final AtomicInteger uniqDocs  = new AtomicInteger(0);
        final AtomicInteger uniqPosts = new AtomicInteger(0);
        final AtomicInteger dupeDocs  = new AtomicInteger(0);

        final ConcurrentMap<String, String> postHashes = new ConcurrentHashMap<>();

        try (final Stream<Path> pathStream = Files.walk(Paths.get(srcPath), FileVisitOption.FOLLOW_LINKS)) {

            pathStream.parallel()
                    .filter(p -> !p.toFile().isDirectory())
                    .filter(p -> p.toString().contains("viewtopic.php"))
                    .forEach(srcFile -> {
                        boolean uniquePostsInCurrentFile = false;
                        String filename = srcFile.toString();
                        Optional<String> html = loadFile(srcFile);
                        if (html.isPresent()) {
                            Document document = Jsoup.parse(html.get());
                            for(Element post : document.select("div.blockpost")) {
                                Optional<String> postId = extractIdFromPost(post);
                                if (postId.isPresent()) {
                                    String postBody = post.select("div.postmsg").stream()
                                            .filter(e -> !e.hasClass("postsignature"))
                                            .map(Element::text)
                                            .map(s -> s.replaceAll("\\s", " "))
                                            .collect(Collectors.joining(" "));

                                    String postHash = buildPostHash(postId.get(), postBody);


                                    if (postHashes.putIfAbsent(postHash, EMPTY) == null) {
                                        uniquePostsInCurrentFile = true;
                                        uniqPosts.incrementAndGet();
                                    }
                                }
                            }
                        }

                        String subpath = filename.substring(srcPath.length() + 1, filename.length());
                        if (uniquePostsInCurrentFile) {
                            // At least one post we've never seen before
                            uniqDocs.incrementAndGet();
                            Path destFile = Paths.get(destPath, subpath);
                            try {
                                System.out.println("UNIQ: Copying " + subpath + " to " + destFile.toString());
                                FileUtils.copyFile(srcFile.toFile(), destFile.toFile());
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                        } else {
                            // File contains just Dupes
                            System.out.println("DUPE: " + subpath);
                            dupeDocs.incrementAndGet();
                        }
                    });

        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

        System.out.println("Total UNIQ posts: " + uniqPosts.get());
        System.out.println("Total documents with >1 UNIQ post: " + uniqDocs.get());
        System.out.println("Total documents will only DUPE posts: " + dupeDocs.get());
    }


    // Dedup the evolution-forums dataset: walk the directory tree, generate a hash
    // by combining postId and SHA256 of file body. If any file contains a post we
    // havent seen yet, copy it to the destination directory.
    public static void main(String[] args) {
        DeduplicateEntities.preprocessEvolutionForums(args[0], args[1]);
    }
}

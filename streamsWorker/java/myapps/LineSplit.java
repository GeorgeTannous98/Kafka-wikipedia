// todo: change the package with your newly named package
// todo: add in pom.xml the proper plugins to run this maven code including apache.kafka plugins and main class configurations
package myapps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class LineSplit {

    private static String A ;

    public static void main(String[] args) {
//        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
//        Calendar cal = Calendar.getInstance();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-linesplit");

        // todo: check port
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        final StreamsBuilder builder = new StreamsBuilder();

        // todo: change input and output topics
        KStream<String, JsonNode> source = builder.stream("wikipedia-events", Consumed.with(Serdes.String(),
                Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer())));
        /*
        * By having the source now we can run some of its required methods to find the needed messages we are looking for
        * 1. we printed out the wikipedia pages written by a bot that are new pages.
        * 2. we printed out the wikipedia pages written by a bot that are now edited.
        * 3. we printed out the wikipedia pages written by a human that are new pages.
        * 4. we printed out the wikipedia pages written by a human that are now edited.
        * */
        source.filter((key,value)-> value.get("bot").asBoolean() && Objects.equals(value.get("type").asText(), "new"))
      /* 1   */          .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofHours(1)))
                .count()
                .toStream()
                .map((wk,v)->new KeyValue<>(A=wk.key(),v))
                .peek((key,value) -> System.out.println((key.indexOf(".") == 2 ? "number of NEW pages of BOT user-type with url of LANGUAGE "+key.charAt(0)+key.charAt(1)+ ": "
                        + value: "no language found" )))
                .map((wk,v)->new KeyValue<>(A,A))
                .to("pipe-1", Produced.with(Serdes.String(),Serdes.String()));
//                        Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer())));


        source.filter((key,value)-> !value.get("bot").asBoolean() && Objects.equals(value.get("type").asText(), "new"))
       /*2     */         .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofHours(1)))
                .count()
                .toStream()
                .map((wk,v)->new KeyValue<>(A=wk.key(),v))
                .peek((key,value) -> System.out.println((key.indexOf(".") == 2 ? "number of NEW pages of HUMAN user-type with url of LANGUAGE "+key.charAt(0)+key.charAt(1)+ ": "
                        + value: "no language found" )))
                .map((wk,v)->new KeyValue<>(A,A))
                    .to("pipe-2",Produced.with(Serdes.String(),Serdes.String()));


        source.filter((key,value)-> value.get("bot").asBoolean() && Objects.equals(value.get("type").asText(), "edit"))
        /*3     */           .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofHours(1)))
                .count()
                .toStream()
                .map((wk,v)->new KeyValue<>(A=wk.key(),v))
                .peek((key,value) -> System.out.println((key.indexOf(".") == 2 ? "number of EDITED pages of BOT user-type with url of LANGUAGE "+key.charAt(0)+key.charAt(1)+ ": "
                 + value: "no language found" )))
                .map((wk,v)->new KeyValue<>(A,A))
                .to("pipe-3", Produced.with(Serdes.String(),Serdes.String()));


        source.filter((key,value)-> !value.get("bot").asBoolean() && Objects.equals(value.get("type").asText(), "edit"))
        /*4     */       .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofHours(1)))
                .count()
                .toStream()
                .map((wk,v)->new KeyValue<>(A=wk.key(),v))
                .peek((key,value) -> System.out.println((key.indexOf(".") == 2 ? "number of EDITED pages of HUMAN user-type with url of LANGUAGE "+key.charAt(0)+key.charAt(1)+ ": "
                        + value: "no language found" )))
                .map((wk,v)->new KeyValue<>(A,A))
                .to("pipe-4",Produced.with(Serdes.String(),Serdes.String()));


        /*here we build our streams event of 1 day duration*/

        source.filter((key,value)-> value.get("bot").asBoolean() && Objects.equals(value.get("type").asText(), "new"))
                /* 1   */          .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofHours(24)))
                .count()
                .toStream()
                .map((wk,v)->new KeyValue<>(A=wk.key(),v))
                .peek((key,value) -> System.out.println((key.indexOf(".") == 2 ? "number of NEW pages of BOT user-type with url of LANGUAGE "+key.charAt(0)+key.charAt(1)+ ": "
                        + value: "no language found" )))
                .map((wk,v)->new KeyValue<>(A,A))
                .to("pipe-5", Produced.with(Serdes.String(),Serdes.String()));
//                        Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer())));


        source.filter((key,value)-> !value.get("bot").asBoolean() && Objects.equals(value.get("type").asText(), "new"))
                /*2     */         .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofHours(24)))
                .count()
                .toStream()
                .map((wk,v)->new KeyValue<>(A=wk.key(),v))
                .peek((key,value) -> System.out.println((key.indexOf(".") == 2 ? "number of NEW pages of HUMAN user-type with url of LANGUAGE "+key.charAt(0)+key.charAt(1)+ ": "
                        + value: "no language found" )))
                .map((wk,v)->new KeyValue<>(A,A))
                .to("pipe-6",Produced.with(Serdes.String(),Serdes.String()));


        source.filter((key,value)-> value.get("bot").asBoolean() && Objects.equals(value.get("type").asText(), "edit"))
                /*3     */           .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofHours(24)))
                .count()
                .toStream()
                .map((wk,v)->new KeyValue<>(A=wk.key(),v))
                .peek((key,value) -> System.out.println((key.indexOf(".") == 2 ? "number of EDITED pages of BOT user-type with url of LANGUAGE "+key.charAt(0)+key.charAt(1)+ ": "
                        + value: "no language found" )))
                .map((wk,v)->new KeyValue<>(A,A))
                .to("pipe-7", Produced.with(Serdes.String(),Serdes.String()));


        source.filter((key,value)-> !value.get("bot").asBoolean() && Objects.equals(value.get("type").asText(), "edit"))
                /*4     */       .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofHours(24)))
                .count()
                .toStream()
                .map((wk,v)->new KeyValue<>(A=wk.key(),v))
                .peek((key,value) -> System.out.println((key.indexOf(".") == 2 ? "number of EDITED pages of HUMAN user-type with url of LANGUAGE "+key.charAt(0)+key.charAt(1)+ ": "
                        + value: "no language found" )))
                .map((wk,v)->new KeyValue<>(A,A))
                .to("pipe-8",Produced.with(Serdes.String(),Serdes.String()));


        /*bulding kafka streams for 1 week duration*/
        source.filter((key,value)-> value.get("bot").asBoolean() && Objects.equals(value.get("type").asText(), "new"))
                /* 1   */          .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofDays(7)))
                .count()
                .toStream()
                .map((wk,v)->new KeyValue<>(A=wk.key(),v))
                .peek((key,value) -> System.out.println((key.indexOf(".") == 2 ? "number of NEW pages of BOT user-type with url of LANGUAGE "+key.charAt(0)+key.charAt(1)+ ": "
                        + value: "no language found" )))
                .map((wk,v)->new KeyValue<>(A,A))
                .to("pipe-9", Produced.with(Serdes.String(),Serdes.String()));
//                        Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer())));


        source.filter((key,value)-> !value.get("bot").asBoolean() && Objects.equals(value.get("type").asText(), "new"))
                /*2     */         .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofDays(7)))
                .count()
                .toStream()
                .map((wk,v)->new KeyValue<>(A=wk.key(),v))
                .peek((key,value) -> System.out.println((key.indexOf(".") == 2 ? "number of NEW pages of HUMAN user-type with url of LANGUAGE "+key.charAt(0)+key.charAt(1)+ ": "
                        + value: "no language found" )))
                .map((wk,v)->new KeyValue<>(A,A))
                .to("pipe-10",Produced.with(Serdes.String(),Serdes.String()));


        source.filter((key,value)-> value.get("bot").asBoolean() && Objects.equals(value.get("type").asText(), "edit"))
                /*3     */           .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofDays(7)))
                .count()
                .toStream()
                .map((wk,v)->new KeyValue<>(A=wk.key(),v))
                .peek((key,value) -> System.out.println((key.indexOf(".") == 2 ? "number of EDITED pages of BOT user-type with url of LANGUAGE "+key.charAt(0)+key.charAt(1)+ ": "
                        + value: "no language found" )))
                .map((wk,v)->new KeyValue<>(A,A))
                .to("pipe-11", Produced.with(Serdes.String(),Serdes.String()));


        source.filter((key,value)-> !value.get("bot").asBoolean() && Objects.equals(value.get("type").asText(), "edit"))
                /*4     */       .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofDays(7)))
                .count()
                .toStream()
                .map((wk,v)->new KeyValue<>(A=wk.key(),v))
                .peek((key,value) -> System.out.println((key.indexOf(".") == 2 ? "number of EDITED pages of HUMAN user-type with url of LANGUAGE "+key.charAt(0)+key.charAt(1)+ ": "
                        + value: "no language found" )))
                .map((wk,v)->new KeyValue<>(A,A))
                .to("pipe-12",Produced.with(Serdes.String(),Serdes.String()));


        /*building the kafka stream events for 1 month*/
        source.filter((key,value)-> value.get("bot").asBoolean() && Objects.equals(value.get("type").asText(), "new"))
                /* 1   */          .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofDays(30)))
                .count()
                .toStream()
                .map((wk,v)->new KeyValue<>(A=wk.key(),v))
                .peek((key,value) -> System.out.println((key.indexOf(".") == 2 ? "number of NEW pages of BOT user-type with url of LANGUAGE "+key.charAt(0)+key.charAt(1)+ ": "
                        + value: "no language found" )))
                .map((wk,v)->new KeyValue<>(A,A))
                .to("pipe-13", Produced.with(Serdes.String(),Serdes.String()));
//                        Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer())));


        source.filter((key,value)-> !value.get("bot").asBoolean() && Objects.equals(value.get("type").asText(), "new"))
                /*2     */         .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofDays(30)))
                .count()
                .toStream()
                .map((wk,v)->new KeyValue<>(A = wk.key(),v))
                .peek((key,value) -> System.out.println((key.indexOf(".") == 2 ? "number of NEW pages of HUMAN user-type with url of LANGUAGE "+key.charAt(0)+key.charAt(1)+ ": "
                        + value: "no language found" )))
                .map((wk,v)->new KeyValue<>(A,A))
                .to("pipe-14",Produced.with(Serdes.String(),Serdes.String()));


        source.filter((key,value)-> value.get("bot").asBoolean() && Objects.equals(value.get("type").asText(), "edit"))
                /*3     */           .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofDays(30)))
                .count()
                .toStream()
                .map((wk,v)->new KeyValue<>( A = wk.key(),v))
                .peek((key,value) -> System.out.println((key.indexOf(".") == 2 ? "number of EDITED pages of BOT user-type with url of LANGUAGE "+key.charAt(0)+key.charAt(1)+ ": "
                        + value: "no language found" )))
                .map((wk,v)->new KeyValue<>(A,A))
                .to("pipe-15", Produced.with(Serdes.String(),Serdes.String()));


        source.filter((key,value)-> !value.get("bot").asBoolean() && Objects.equals(value.get("type").asText(), "edit"))
                /*4     */       .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofDays(30)))
                .count()
                .toStream()
                .map((wk,v)->new KeyValue<>(A=wk.key(),v))
                .peek((key,value) -> System.out.println((key.indexOf(".") == 2 ? "number of EDITED pages of HUMAN user-type with url of LANGUAGE "+key.charAt(0)+key.charAt(1)+ ": "
                        + value: "no language found" )))
                .map((wk,v)->new KeyValue<>(A,A))
                .to("pipe-16",Produced.with(Serdes.String(),Serdes.String()));




        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
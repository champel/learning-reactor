package cat.champel.learning.reactor;

import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.publisher.SynchronousSink;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
public class LearningTest {

	private String[] things;
	
	
	@BeforeEach
	public void setup() {
		things = new String[] { "thing1", "thing2", "thing3", "thing4", "thing5", "thing6", "thing7", "thing8", "thing9" };
	}

	@Test
	public void testFromArray() {
		subscribeExpectThingsAndCompletion(Flux.fromArray(things));
	}
	
	@Test
	public void testPublisher() {
		Publisher<String> publisher = new Publisher<String>() {

			@Override
			public void subscribe(Subscriber<? super String> s) {
				s.onSubscribe(new Subscription() {
					int currentSequence = 0;
					int requested = 0;

					@Override
					public void request(long n) {
						requested += n;
						System.out.println(currentSequence + " -> " + n + " (" + requested + ")");
						for (int i = 0; i < n; i++) {
							if (currentSequence == things.length) {
								s.onComplete();
								break;
							} else {
								s.onNext(things[currentSequence++]);
							}
						}
					}

					@Override
					public void cancel() {
						// Not tested
					}
				});
			}
		};
		subscribeExpectThingsAndCompletion(Flux.from(publisher)); 
	}

	@Test
	public void testFluxSink() {
		Consumer<FluxSink<String>> emitter = new Consumer<FluxSink<String>>() {
			private int currentSequence = 0;

			@Override
			public void accept(FluxSink<String> sink) {
				sink.onRequest((n) -> {
					for (int i = 0; i < n; i++) {
						if (currentSequence == things.length) {
							sink.complete();
							break;
						} else {
							sink.next(things[currentSequence++]);
						}
					}
				});
				sink.onCancel(() -> {
					// Not tested
				});
			}
		};
		subscribeExpectThingsAndCompletion(Flux.create(emitter, OverflowStrategy.ERROR)); 
	}

	private Integer nextStep(int currentStep, SynchronousSink<String> sink) {
		if (currentStep == things.length) {
			sink.complete();
		} else {
			sink.next(things[currentStep++]);
		}
		return currentStep;
	}

	@Test
	public void testSynchronousSink() {
		Consumer<SynchronousSink<String>> generator = new Consumer<SynchronousSink<String>>() {
			int currentSequence = 0;

			@Override
			public void accept(SynchronousSink<String> sink) {
				nextStep(currentSequence++, sink);
			}
		};
		subscribeExpectThingsAndCompletion(Flux.generate(generator)); 
	}

	@Test
	public void testSynchronousSinkWithState() {
		subscribeExpectThingsAndCompletion(Flux.generate(() -> 0, this::nextStep)); 
	}

	@Test
	public void testSynchronousSinkWithStateAndCleanup(@Mock Consumer<Integer> cleanup) {
		subscribeExpectThingsAndCompletion(Flux.generate(() -> 0, this::nextStep, cleanup)); 
		Mockito.verify(cleanup).accept(things.length);
	}

	private void subscribeExpectThingsAndCompletion(Flux<String> source) throws AssertionError {
		Flux<String> sourceWithBackPresure = source.limitRate(4);
		
		StepVerifier.create(sourceWithBackPresure)
			.expectNext("thing1") 
			.expectNext("thing2")
			.expectNext("thing3")
			.expectNext("thing4")
			.expectNext("thing5")
			.expectNext("thing6")
			.expectNext("thing7")
			.expectNext("thing8")
			.expectNext("thing9")
			.expectComplete() 
			.verify();
	}
}

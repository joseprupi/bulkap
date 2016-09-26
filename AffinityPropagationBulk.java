package org.apache.flink.graph.examples;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.examples.data.AffinityPropagationData;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * Created by joseprubio on 9/22/16.
 */

public class AffinityPropagationBulk {

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Get input similarities Tuple3<src, target, similarity>
		DataSet<Tuple3<Long, Long, Double>> similarities =
			AffinityPropagationData.getTuples(env);

		// Init input to iteration
		DataSet<Message> initMessages = similarities.map(new InitMessage());

		// Iterate
		IterativeDataSet<Message> messages = initMessages.iterate(2);

		// Start responsibility message calculation
		// r(i,k) <- s(i,k) - max {a(i,K) + s(i,K)} st K != k
		DataSet<Message> responsibilities = similarities

			// Get a list of a(i,K) + s(i,K) values joining similarities with messages
			.join(messages)
				.where("f0","f1")
				.equalTo("src","trg")
				.with(new JoinFunction<Tuple3<Long, Long, Double>, Message, Tuple3<Long, Long, Double>>() {
					public Tuple3<Long, Long, Double> join(Tuple3<Long, Long, Double> left, Message right) throws Exception {
						return new Tuple3<Long, Long, Double>(left.f0, left.f1, left.f2+right.availability);
					}
				})

			// Get a dataset with 2 higher values
			.groupBy("f1").sortGroup("f2", Order.DESCENDING)
			.first(2)

			// Create a Tuple4<Trg, MaxValue, MaxNeighbour, SecondMaxValue> reducing the 2 tuples with higher values
			.groupBy("f1")
			.reduceGroup(new responsibilityReduceGroup())

			// Calculate the R messages "r(i,k) <- s(i,k) - value" getting "value" joining
			// similarities with previous tuple
			.leftOuterJoin(similarities)
				.where(0)
				.equalTo(1)
				.with(new responsibilityValue());

		// Start availability message calculation
		// a(i,k) <- min {0, r(k,k) + sum{max{0,r(I,k)}} I st I not in {i,k}
		// a(k,k) <- sum{max{0,r(I,k)} I st I not in {i,k}

		DataSet<Message> availabilities = responsibilities

			// Get the sum of the responsibilities and the self responsibility per target
			.groupBy("trg")
			.reduceGroup(new availabilityReduceGroup())

			// Calculate the availability
			.leftOuterJoin(responsibilities)
				.where("f0")
				.equalTo("trg")
				.with(new availabilityValue());

		// End iteration
		DataSet<Message> finalMessages =  messages.closeWith(availabilities);

		for(Message m : finalMessages.collect()){
			System.out.println("Src: " + m.src + " Trg: " + m.trg + " A: " + m.availability + " R: "
				+ m.responsibility);
		}

	}

	private static class InitMessage implements MapFunction<Tuple3<Long, Long, Double>, Message> {
		@Override
		public Message map(Tuple3<Long, Long, Double> in) {
			return new Message(in.f0, in.f1);
		}
	}

	// Create a Tuple4<Trg, MaxValue, MaxNeighbour, SecondMaxValue> reducing the 2 tuples with the max values
	private static class responsibilityReduceGroup
		implements GroupReduceFunction<Tuple3<Long, Long, Double>, Tuple4<Long, Double, Long, Double>> {

		@Override
		public void reduce(Iterable<Tuple3<Long, Long, Double>> maxValues,
							Collector<Tuple4<Long, Double, Long, Double>> out) throws Exception {

			Long maxNeighbour = Long.valueOf(0);
			Long trg = Long.valueOf(0);
			double maxValue = 0;
			double secondMaxValue = 0;

			for (Tuple3<Long, Long, Double> val : maxValues) {
				if(val.f2 > maxValue){
					secondMaxValue = maxValue;
					maxValue = val.f2;
					maxNeighbour = val.f0;
					trg = val.f1;
				}else{
					secondMaxValue = val.f2;
				}
			}

			Tuple4<Long, Double, Long, Double> result = new Tuple4<>();
			result.f0 = trg;
			result.f1 = maxValue;
			result.f2 = maxNeighbour;
			result.f3 = secondMaxValue;

			out.collect(result);

		}
	}

	// Substract each respo
	private static class responsibilityValue
		implements JoinFunction<Tuple4<Long, Double, Long, Double>, Tuple3<Long, Long, Double>, Message> {

		//Receives Tuple4<Trg, MaxValue, MaxNeighbour, SecondMaxValue> and Tuple3<src, target, similarity>
		@Override
		public Message join(Tuple4<Long, Double, Long, Double> maxValues, Tuple3<Long, Long, Double> similarity) {

			double responsibility = 0;

			if(similarity.f0 == maxValues.f2){
				responsibility = similarity.f2 - maxValues.f3;
			}else{
				responsibility = similarity.f2 - maxValues.f1;
			}

			return new Message(similarity.f1, similarity.f0, 0, responsibility);
		}
	}

	// Return a Tuple3<Trg, PositiveResponsibilitiesAccumulator, SelfResponsibility>
	private static class availabilityReduceGroup
		implements GroupReduceFunction<Message, Tuple3<Long, Double, Double>> {

		@Override
		public void reduce(Iterable<Message> messages,
						   Collector<Tuple3<Long, Double, Double>> out) throws Exception {

			double accum = 0;
			double selfResponsibility = 0;
			Long trg = Long.valueOf(0);

			for (Message m : messages) {
				if(m.trg == m.src){
					selfResponsibility = m.responsibility;
					trg = m.trg;
				}else{
					if(m.responsibility > 0){
						accum = accum + m.responsibility;
					}
				}
			}

			Tuple3<Long, Double, Double> result = new Tuple3<>();
			result.f0 = trg;
			result.f1 = accum;
			result.f2 = selfResponsibility;

			out.collect(result);

		}
	}

	// Return a Tuple3<Trg, PositiveResponsibilitiesAccumulator, SelfResponsibility>
	private static class availabilityValue
		implements JoinFunction<Tuple3<Long, Double, Double>, Message, Message> {

		@Override
		public Message join(Tuple3<Long, Double, Double> first, Message message) throws Exception {

			Message availability = new Message();
			availability.src = message.trg;
			availability.trg = message.src;

			if(message.trg == message.src){
				availability.availability = first.f1;
			}else{
				availability.availability = first.f1 + first.f2;
			}

			if(message.responsibility > 0) {
				availability.availability = availability.availability - message.responsibility;
			}

			return availability;
		}
	}

	public static class Message implements Serializable {

		public Long src, trg;
		public double responsibility, availability;

		public Message() {}

		public Message(Long src, Long trg) {
			this.src = src;
			this.trg = trg;
		}

		public Message(Long src, Long trg, double availability, double responsibility) {
			this.src = src;
			this.trg = trg;
			this.availability = availability;
			this.responsibility = responsibility;
		}

	}

}

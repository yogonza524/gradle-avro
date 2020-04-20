package com.commerece.avrodemo;

import com.rna.domain.Function;
import com.rna.domain.NeuralNetwork;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class AvroDemoApplicationTests {

	@Test
	void contextLoads() throws IOException {
		var rna = NeuralNetwork
				.newBuilder()
				.setInputs(4)
				.setOutputs(2)
				.setFuncion(Function.HIPERBOLIC)
				.build();

		byte[] serilized = serialize("/home/gonzalo/rna", rna);
		var deserialized = deserialize(serilized);

		System.out.println(deserialized.toString());
	}

	public byte[] serialize(String path, NeuralNetwork rna) {
		DatumWriter<NeuralNetwork> writer = new SpecificDatumWriter<>(NeuralNetwork.class);
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		Encoder jsonEncoder = null;
		byte[] data = new byte[0];
		try {
			jsonEncoder = EncoderFactory.get().jsonEncoder(
					NeuralNetwork.getClassSchema(), stream
			);
			writer.write(rna, jsonEncoder);
			jsonEncoder.flush();
			data = stream.toByteArray();

			Files.write(Path.of(path), data);

			return data;

		} catch (IOException e) {
			e.printStackTrace();
		}
		throw new RuntimeException("Something is wrong");
	}

	public NeuralNetwork deserialize(byte[] data) {
		DatumReader<NeuralNetwork> reader
				= new SpecificDatumReader<>(NeuralNetwork.class);
		Decoder decoder = null;
		try {
			decoder = DecoderFactory.get().jsonDecoder(
					NeuralNetwork.getClassSchema(), new String(data));
			return reader.read(null, decoder);
		} catch (IOException e) {
			e.printStackTrace();
		}
		throw new RuntimeException("Something is wrong");
	}
}

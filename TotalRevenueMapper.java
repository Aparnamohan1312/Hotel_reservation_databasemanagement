package TotalRevenue;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.concurrent.atomic.DoubleAccumulator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class TotalRevenueMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
            throws IOException {

        String input_string = value.toString();
        String[] hotel_booking_data = input_string.split(",");

        if (hotel_booking_data[0].contains("INN")) {
            System.out.printf("Entered customer reservation record", hotel_booking_data[0]);
            String bookingStatusStr = hotel_booking_data[9];

            if (bookingStatusStr.contains("Not_Canceled")) {
                String arrivalMonth = hotel_booking_data[5];
                double avgPricePerRoom = Double.parseDouble(hotel_booking_data[8]);

                double stay_weekdays = Double.parseDouble(hotel_booking_data[2]);
                double stay_weekends = Double.parseDouble(hotel_booking_data[1]);

                double totalRevenue = avgPricePerRoom * (stay_weekdays + stay_weekends);

                int monthValue = Integer.parseInt(arrivalMonth);
                String month = getMonthName(monthValue);

                output.collect(new Text(month), new DoubleWritable(totalRevenue));
            }
        } else {
            System.out.printf("Entered hotel booking record", hotel_booking_data[0]);
            String arrivalYearStr = hotel_booking_data[3];
            String bookingStatusStr = hotel_booking_data[1];

            if (!isNumeric(arrivalYearStr) || !isNumeric(bookingStatusStr)) {
                System.err.println("Invalid numeric value: arrivalYearStr = " + arrivalYearStr + ", bookingStatusStr = "
                        + bookingStatusStr);
                return;
            }

            // double arrivalYear = Double.parseDouble(arrivalYearStr);
            double bookingStatus = Double.parseDouble(bookingStatusStr);

            if (bookingStatus == 0) {
                String arrivalMonth = hotel_booking_data[4];
                double avgPricePerRoom = Double.parseDouble(hotel_booking_data[11]);

                double stay_weekdays = Double.parseDouble(hotel_booking_data[7]);
                double stay_weekends = Double.parseDouble(hotel_booking_data[8]);

                double totalRevenue = avgPricePerRoom * (stay_weekdays + stay_weekends);

                output.collect(new Text(arrivalMonth), new DoubleWritable(totalRevenue));
            }
        }
    }

    private boolean isNumeric(String str) {
        return str.matches("-?\\d+(\\.\\d+)?");
    }

    private String getMonthName(int monthValue) {
        String[] months = { "January", "February", "March", "April", "May", "June", "July", "August", "September",
                "October", "November", "December" };
        int array_index = monthValue - 1;

        if (array_index >= 0 && array_index < months.length) {
            return months[array_index];
        } else {
            return "Invalid Month";
        }
    }
}
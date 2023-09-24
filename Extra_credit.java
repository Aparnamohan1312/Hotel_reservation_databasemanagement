public class TotalRevenueMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text outputKey = new Text();
    private DoubleWritable outputValue = new DoubleWritable();

    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
            throws IOException {

        String inputString = value.toString();
        String[] hotelBookingData = inputString.split(",");

        if (hotelBookingData[0].contains("INN")) {
            System.out.println("Entered customer reservation record: " + hotelBookingData[0]);
            String bookingStatusStr = hotelBookingData[9];

            if (bookingStatusStr.contains("Not_Canceled")) {
                String arrivalMonth = hotelBookingData[5];
                double avgPricePerRoom = Double.parseDouble(hotelBookingData[8]);

                double stayWeekdays = Double.parseDouble(hotelBookingData[2]);
                double stayWeekends = Double.parseDouble(hotelBookingData[1]);

                double totalRevenue = avgPricePerRoom * (stayWeekdays + stayWeekends);

                int monthValue = Integer.parseInt(arrivalMonth);
                String month = getMonthName(monthValue);

                outputKey.set(month);
                outputValue.set(totalRevenue);
                output.collect(outputKey, outputValue);
            }
        } else {
            System.out.println("Entered hotel booking record: " + hotelBookingData[0]);
            String arrivalYearStr = hotelBookingData[3];
            String bookingStatusStr = hotelBookingData[1];

            if (!isNumeric(arrivalYearStr) || !isNumeric(bookingStatusStr)) {
                System.err.println("Invalid numeric value: arrivalYearStr = " + arrivalYearStr + ", bookingStatusStr = "
                        + bookingStatusStr);
                return;
            }

            double bookingStatus = Double.parseDouble(bookingStatusStr);

            if (bookingStatus == 0) {
                String arrivalMonth = hotelBookingData[4];
                double avgPricePerRoom = Double.parseDouble(hotelBookingData[11]);

                double stayWeekdays = Double.parseDouble(hotelBookingData[7]);
                double stayWeekends = Double.parseDouble(hotelBookingData[8]);

                double totalRevenue = avgPricePerRoom * (stayWeekdays + stayWeekends);

                outputKey.set(arrivalMonth);
                outputValue.set(totalRevenue);
                output.collect(outputKey, outputValue);
            }
        }
    }

    private boolean isNumeric(String str) {
        return str.matches("-?\\d+(\\.\\d+)?");
    }

    private String getMonthName(int monthValue) {
        String[] months = { "January", "February", "March", "April", "May", "June", "July", "August", "September",
                "October", "November", "December" };
        int arrayIndex = monthValue - 1;

        if (arrayIndex >= 0 && arrayIndex < months.length) {
            return months[arrayIndex];
        } else {
            return "Invalid Month";
        }
    }
}

const fs = require('fs');
const CsvParser = require('csv-parse');
const CsvWriter = require('csv-writer').createObjectCsvWriter;
const dataDir = './data/';
const resultsDir = './results/';

const continuouslyCompoundedReturn = (historicalData) => {
  console.log('continuouslyCompoundedReturn() historicalData:', historicalData);
  let dpsr = 0;
  let compoundDPSR = 0;
  let prevPrice = historicalData[0];
  const dayToDPSRMap = {};
  historicalData.forEach((currentPrice, i) => {
    dpsr = Math.log(currentPrice / prevPrice);
    console.log('Day i:', i);
    dayToDPSRMap[i] = dpsr;
    compoundDPSR += dpsr;
    prevPrice = currentPrice;
    console.log('current dspr:', dpsr);
  });

  const avgDPSR = compoundDPSR / historicalData.length;
  console.log(
    'avgDPSR (compoundDPSR / historicalData.length):',
    avgDPSR,
    avgDPSR * 100,
  );
  return { dayToDPSRMap, avgDPSR };
};

const avgDailyVolatility = (historicalData, { dayToDPSRMap, avgDPSR }) => {
  console.log('avgDailyVolatility()');
  const n = historicalData.length;
  const sumOverTotalObservationsUnbiased = Object.keys(dayToDPSRMap).reduce(
    (prev, _, ithDay) => {
      const diff = n - (ithDay + 1);
      return prev + (dayToDPSRMap[String(diff)] - avgDPSR) ** 2;
    },
    0,
  );
  const sumOverTotalObservationsMaxProbEst = Object.keys(dayToDPSRMap).reduce(
    (prev, _, ithDay) => {
      const diff = n - (ithDay + 1);
      return prev + dayToDPSRMap[String(diff)] ** 2;
    },
    0,
  );

  // NOTE: also known as sample daily variance (sdv)
  // NOTE: 1/totalDays(historicalData.length) since each day needs to be accounted for equally (one of our x days)
  // NOTE: 1/totalDays = the weight (of the average i.e. what we are multiplying the weight by)
  const sdvUnbiasedEstimator =
    (1 / (historicalData.length - 1)) * sumOverTotalObservationsUnbiased;
  const sdvMaxLikelihoodEstimate =
    (1 / historicalData.length) * sumOverTotalObservationsMaxProbEst;
  console.log(
    'sample daily variance using unbiased estimator:',
    sdvUnbiasedEstimator,
    sdvUnbiasedEstimator * 100,
  );
  console.log(
    'sample daily variance using max likelihood estimate:',
    sdvMaxLikelihoodEstimate,
    sdvMaxLikelihoodEstimate * 100,
  );

  const historicalDailyVolatilityUnbiased = Math.sqrt(sdvUnbiasedEstimator);
  console.log(
    'historical daily volatility using unbiased estimator:',
    historicalDailyVolatilityUnbiased,
    historicalDailyVolatilityUnbiased * 100,
  );

  const historicalDailyVolatilityMaxProbEst = Math.sqrt(
    sdvMaxLikelihoodEstimate,
  );
  console.log(
    'historical daily volatility using max likelihood estimate:',
    historicalDailyVolatilityMaxProbEst,
    historicalDailyVolatilityMaxProbEst * 100,
  );

  return {
    sdvUnbiasedEstimator: sdvUnbiasedEstimator * 100,
    sdvMaxLikelihoodEstimate: sdvMaxLikelihoodEstimate * 100,
    historicalDailyVolatilityUnbiased: historicalDailyVolatilityUnbiased * 100,
    historicalDailyVolatilityMaxProbEst:
      historicalDailyVolatilityMaxProbEst * 100,
  };
};

const processPriceData = (fileName) => {
  return new Promise((resolve, reject) => {
    let historicalData = [];
    fs.createReadStream(`${__dirname}/data/${fileName}`)
      .pipe(CsvParser({ delimiter: ',' }))
      .on('data', (csvRow) => {
        console.log('csvRow:', csvRow[1]);
        if (csvRow[1] !== 'price') {
          historicalData.push(parseFloat(csvRow[1]));
        }
      })
      .on('end', () => {
        console.log('end');
        const dpsrData = continuouslyCompoundedReturn(historicalData);
        const advData = avgDailyVolatility(historicalData, dpsrData);
        resolve({
          ...advData,
          fileName,
          avgDPSR: dpsrData.avgDPSR,
        });
      });
  });
};

const compute = () => {
  return new Promise(async (resolve, reject) => {
    let resultsCSVData = [];
    for (const fileName of fs.readdirSync(dataDir)) {
      const csvData = await processPriceData(fileName);
      resultsCSVData.push(csvData);
    }
    resolve(resultsCSVData);
  });
};

(async () => {
  const resultsCSVData = await compute();
  console.log('resultsCSVData:', resultsCSVData);

  resultsCSVData.sort((d1, d2) => {
    return (
      d2.historicalDailyVolatilityUnbiased -
      d1.historicalDailyVolatilityUnbiased
    );
  });

  const path = `${__dirname}/${resultsDir}crypto_historical_volatility.csv`;
  const csvWriter = CsvWriter({
    path,
    header: [
      { id: 'fileName', title: 'File Name' },
      { id: 'avgDPSR', title: 'Average DPSR' },
      {
        id: 'sdvUnbiasedEstimator',
        title: 'Sample Daily Variance (unbiased estimator) [in %]',
      },
      {
        id: 'sdvMaxLikelihoodEstimate',
        title: 'Sample Daily Variance (maximum likelihood estimate) [in %]',
      },
      {
        id: 'historicalDailyVolatilityUnbiased',
        title: 'Historical Daily Volatility (unbiased estimator) [in %]',
      },
      {
        id: 'historicalDailyVolatilityMaxProbEst',
        title:
          'Historical Daily Volatility (maximum likelihood estimate) [in %]',
      },
    ],
  });

  csvWriter
    .writeRecords(resultsCSVData)
    .then(() => console.log(`Results successfully written to ${path}`));
})();

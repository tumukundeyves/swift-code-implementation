// Project structure
/*
swift-code-service/
├── src/
│   ├── controllers/
│   │   └── swiftCodeController.js
│   ├── models/
│   │   └── swiftCode.js
│   ├── routes/
│   │   └── swiftCodeRoutes.js
│   ├── services/
│   │   └── swiftCodeService.js
│   ├── utils/
│   │   └── dataParser.js
│   ├── config/
│   │   └── database.js
│   └── app.js
├── package.json
└── server.js
*/

// package.json
{
  "name": "swift-code-service",
  "version": "1.0.0",
  "description": "SWIFT Code Management API",
  "main": "server.js",
  "scripts": {
    "start": "node server.js",
    "dev": "nodemon server.js",
    "parse": "node src/utils/dataParser.js"
  },
  "dependencies": {
    "cors": "^2.8.5",
    "csv-parser": "^3.0.0",
    "dotenv": "^16.0.3",
    "express": "^4.18.2",
    "mongoose": "^7.1.0"
  },
  "devDependencies": {
    "nodemon": "^2.0.22"
  }
}

// server.js
const app = require('./src/app');
const mongoose = require('mongoose');
const config = require('./src/config/database');

const PORT = process.env.PORT || 3000;

// Connect to MongoDB
mongoose.connect(config.mongoURI)
  .then(() => {
    console.log('Connected to MongoDB');
    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
    });
  })
  .catch(err => {
    console.error('Failed to connect to MongoDB', err);
    process.exit(1);
  });

// src/app.js
const express = require('express');
const cors = require('cors');
const swiftCodeRoutes = require('./routes/swiftCodeRoutes');

const app = express();

// Middleware
app.use(cors());
app.use(express.json());

// Routes
app.use('/v1/swift-codes', swiftCodeRoutes);

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ message: 'Something went wrong!' });
});

module.exports = app;

// src/config/database.js
module.exports = {
  mongoURI: process.env.MONGODB_URI || 'mongodb://localhost:27017/swift-codes'
};

// src/models/swiftCode.js
const mongoose = require('mongoose');

const swiftCodeSchema = new mongoose.Schema({
  swiftCode: {
    type: String,
    required: true,
    unique: true,
    trim: true,
    uppercase: true
  },
  bankName: {
    type: String,
    required: true,
    trim: true
  },
  address: {
    type: String,
    required: true,
    trim: true
  },
  countryISO2: {
    type: String,
    required: true,
    trim: true,
    uppercase: true
  },
  countryName: {
    type: String, 
    required: true,
    trim: true,
    uppercase: true
  },
  isHeadquarter: {
    type: Boolean,
    required: true
  }
});

// Index for faster querying
swiftCodeSchema.index({ swiftCode: 1 });
swiftCodeSchema.index({ countryISO2: 1 });
swiftCodeSchema.index({ swiftCode: 1, isHeadquarter: 1 });

const SwiftCode = mongoose.model('SwiftCode', swiftCodeSchema);

module.exports = SwiftCode;

// src/routes/swiftCodeRoutes.js
const express = require('express');
const swiftCodeController = require('../controllers/swiftCodeController');

const router = express.Router();

// GET routes
router.get('/:swiftCode', swiftCodeController.getSwiftCodeDetails);
router.get('/country/:countryISO2', swiftCodeController.getSwiftCodesByCountry);

// POST route
router.post('/', swiftCodeController.addSwiftCode);

// DELETE route
router.delete('/:swiftCode', swiftCodeController.deleteSwiftCode);

module.exports = router;

// src/controllers/swiftCodeController.js
const swiftCodeService = require('../services/swiftCodeService');

exports.getSwiftCodeDetails = async (req, res, next) => {
  try {
    const { swiftCode } = req.params;
    const result = await swiftCodeService.getSwiftCodeDetails(swiftCode);
    
    if (!result) {
      return res.status(404).json({ message: 'SWIFT code not found' });
    }
    
    res.status(200).json(result);
  } catch (error) {
    next(error);
  }
};

exports.getSwiftCodesByCountry = async (req, res, next) => {
  try {
    const { countryISO2 } = req.params;
    const result = await swiftCodeService.getSwiftCodesByCountry(countryISO2.toUpperCase());
    
    if (!result) {
      return res.status(404).json({ message: 'Country not found' });
    }
    
    res.status(200).json(result);
  } catch (error) {
    next(error);
  }
};

exports.addSwiftCode = async (req, res, next) => {
  try {
    const swiftCodeData = req.body;
    
    // Validate required fields
    const requiredFields = ['swiftCode', 'bankName', 'address', 'countryISO2', 'countryName', 'isHeadquarter'];
    for (const field of requiredFields) {
      if (!swiftCodeData[field] && swiftCodeData[field] !== false) {
        return res.status(400).json({ message: `Missing required field: ${field}` });
      }
    }
    
    // Ensure uppercase for country fields
    swiftCodeData.countryISO2 = swiftCodeData.countryISO2.toUpperCase();
    swiftCodeData.countryName = swiftCodeData.countryName.toUpperCase();
    
    const result = await swiftCodeService.addSwiftCode(swiftCodeData);
    res.status(201).json({ message: 'SWIFT code added successfully' });
  } catch (error) {
    if (error.code === 11000) { // MongoDB duplicate key error
      res.status(409).json({ message: 'SWIFT code already exists' });
    } else {
      next(error);
    }
  }
};

exports.deleteSwiftCode = async (req, res, next) => {
  try {
    const { swiftCode } = req.params;
    const result = await swiftCodeService.deleteSwiftCode(swiftCode);
    
    if (result.deletedCount === 0) {
      return res.status(404).json({ message: 'SWIFT code not found' });
    }
    
    res.status(200).json({ message: 'SWIFT code deleted successfully' });
  } catch (error) {
    next(error);
  }
};

// src/services/swiftCodeService.js
const SwiftCode = require('../models/swiftCode');

exports.getSwiftCodeDetails = async (swiftCode) => {
  // Find the requested SWIFT code
  const swiftCodeData = await SwiftCode.findOne({ swiftCode: swiftCode.toUpperCase() });
  
  if (!swiftCodeData) {
    return null;
  }
  
  // Format the base response
  const response = {
    address: swiftCodeData.address,
    bankName: swiftCodeData.bankName,
    countryISO2: swiftCodeData.countryISO2,
    countryName: swiftCodeData.countryName,
    isHeadquarter: swiftCodeData.isHeadquarter,
    swiftCode: swiftCodeData.swiftCode
  };
  
  // If this is a headquarters, include branches
  if (swiftCodeData.isHeadquarter) {
    // Get first 8 characters of the SWIFT code to find branches
    const bankPrefix = swiftCodeData.swiftCode.substring(0, 8);
    
    // Find all branches of this bank (excluding the HQ itself)
    const branches = await SwiftCode.find({
      swiftCode: { $ne: swiftCodeData.swiftCode },
      swiftCode: { $regex: `^${bankPrefix}` },
      isHeadquarter: false
    });
    
    response.branches = branches.map(branch => ({
      address: branch.address,
      bankName: branch.bankName,
      countryISO2: branch.countryISO2,
      isHeadquarter: branch.isHeadquarter,
      swiftCode: branch.swiftCode
    }));
  }
  
  return response;
};

exports.getSwiftCodesByCountry = async (countryISO2) => {
  // Find all SWIFT codes for the given country
  const swiftCodes = await SwiftCode.find({ countryISO2: countryISO2.toUpperCase() });
  
  if (swiftCodes.length === 0) {
    return null;
  }
  
  // Format the response
  const response = {
    countryISO2: countryISO2.toUpperCase(),
    countryName: swiftCodes[0].countryName, // All records for this country should have the same name
    swiftCodes: swiftCodes.map(code => ({
      address: code.address,
      bankName: code.bankName,
      countryISO2: code.countryISO2,
      isHeadquarter: code.isHeadquarter,
      swiftCode: code.swiftCode
    }))
  };
  
  return response;
};

exports.addSwiftCode = async (swiftCodeData) => {
  return await SwiftCode.create(swiftCodeData);
};

exports.deleteSwiftCode = async (swiftCode) => {
  return await SwiftCode.deleteOne({ swiftCode: swiftCode.toUpperCase() });
};

// src/utils/dataParser.js
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const mongoose = require('mongoose');
const SwiftCode = require('../models/swiftCode');
const config = require('../config/database');

// Path to the CSV file - update this to match your file location
const CSV_FILE_PATH = path.resolve(__dirname, '../../../data/swift_codes.csv');

// Parse SWIFT codes from CSV file
async function parseAndStoreSwiftCodes() {
  try {
    // Connect to MongoDB
    await mongoose.connect(config.mongoURI);
    console.log('Connected to MongoDB');
    
    // Clear existing data (optional)
    await SwiftCode.deleteMany({});
    console.log('Cleared existing SWIFT code data');
    
    const swiftCodes = [];
    
    // Create a stream to read and parse the CSV file
    fs.createReadStream(CSV_FILE_PATH)
      .pipe(csv())
      .on('data', (row) => {
        // Extract and transform data
        const swiftCode = row.SWIFT || row.swift_code || '';
        const isHeadquarter = swiftCode.endsWith('XXX');
        
        // Format countries as uppercase
        const countryISO2 = (row.COUNTRY_ISO || row.country_iso || '').toUpperCase();
        const countryName = (row.COUNTRY_NAME || row.country_name || '').toUpperCase();
        
        // Create a SWIFT code record
        const swiftCodeRecord = {
          swiftCode: swiftCode,
          bankName: row.BANK_NAME || row.bank_name || '',
          address: row.ADDRESS || row.address || '',
          countryISO2: countryISO2,
          countryName: countryName,
          isHeadquarter: isHeadquarter
        };
        
        swiftCodes.push(swiftCodeRecord);
      })
      .on('end', async () => {
        // Insert all parsed records to the database
        if (swiftCodes.length > 0) {
          await SwiftCode.insertMany(swiftCodes);
          console.log(`Successfully imported ${swiftCodes.length} SWIFT code records`);
        } else {
          console.log('No data found to import');
        }
        
        // Disconnect from MongoDB
        mongoose.disconnect();
      });
  } catch (error) {
    console.error('Error parsing SWIFT codes:', error);
    process.exit(1);
  }
}

// Execute if this file is run directly
if (require.main === module) {
  parseAndStoreSwiftCodes();
}

module.exports = { parseAndStoreSwiftCodes };
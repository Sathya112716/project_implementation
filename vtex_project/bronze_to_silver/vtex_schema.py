# Databricks notebook source file
from pyspark.sql.types import StructType, StructField, BooleanType, StringType, ArrayType, FloatType, LongType, MapType, IntegerType, DoubleType, TimestampType

# COMMAND ----------

cancellationData_schema = StructType([
    StructField("RequestedByUser", BooleanType(), True),
    StructField("RequestedBySystem", BooleanType(), True),
    StructField("RequestedBySellerNotification", BooleanType(), True),
    StructField("RequestedByPaymentNotification", BooleanType(), True),
    StructField("Reason", StringType(), True),
    StructField("CancellationDate", StringType(), True)
])
invoice_data_schema = StructType([
    StructField("userPaymentInfo", StringType(), True),
    StructField("address", StructType([
        StructField("postalCode", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("street", StringType(), True),
        StructField("number", StringType(), True),
        StructField("neighborhood", StringType(), True),
        StructField("complement", StringType(), True),
        StructField("reference", StringType(), True),
        StructField("geoCoordinates", ArrayType(DoubleType()), True)
    ]), True),
    StructField("invoiceSubject", StringType(), True)
])
commercialConditionData_schema = StructType([
    StructField("id", StringType(), True),
    StructField("parentOrderId", StringType(), True),
    StructField("reason", StringType(), True)
])
garantia_estendida_schema = StructType([
    StructField("name", StringType(), True),
    StructField("refId", StringType(), True),
    StructField("id", StringType(), True),
    StructField("productId", StringType(), True),
    StructField("productWeight", IntegerType(), True),
    StructField("lineID", IntegerType(), True),
    StructField("model", StringType(), True)
])

takeback_schema = StructType([
    StructField("nome", StringType(), True),
    StructField("productName", StringType(), True),
    StructField("valor", StringType(), True),
    StructField("material", StringType(), True)
])

content_schema = StructType([
    StructField("takeback", StringType(), True),  # JSON string, will be parsed later
    StructField("garantia-estendida", StringType(), True)  # JSON string, will be parsed later
])

array_schema = StructType([
    StructField("name", StringType(), True),
    StructField("content", content_schema, True)
])
subscriptionData_schema = StructType([
    StructField("SubscriptionGroupId", StringType(), True),
    StructField("Subscriptions", ArrayType(
        StructType([
            StructField("ExecutionCount", IntegerType(), True),
            StructField("PriceAtSubscriptionDate", DoubleType(), True),
            StructField("ItemIndex", IntegerType(), True),
            StructField("Plan", StructType([
                StructField("type", StringType(), True),
                StructField("frequency", StructType([
                    StructField("periodicity", StringType(), True),
                    StructField("interval", IntegerType(), True)
                ]), True),
                StructField("validity", StructType([
                    StructField("begin", StringType(), True),
                    StructField("end", StringType(), True)
                ]), True)
            ]), True)
        ])
    ), True)
])
item_schema = StructType([
    StructField("Id", StringType(), True),
    StructField("Seller", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("SkuName", StringType(), True),
    StructField("ProductId", StringType(), True),
    StructField("RefId", StringType(), True),
    StructField("Ean", StringType(), True),
    StructField("ImageUrl", StringType(), True),
    StructField("DetailUrl", StringType(), True),
    StructField("AssemblyOptions", StringType(), True)
])

composition_schema = StructType([
    StructField("Items", ArrayType(item_schema), True)
])
additional_info_schema = StructType([
    StructField("brandName", StringType(), True),
    StructField("brandId", StringType(), True),
    StructField("categoriesIds", StringType(), True),
    StructField("categories", StringType(), True),
    StructField("productClusterId", StringType(), True),
    StructField("commercialConditionId", StringType(), True),
    StructField("dimension", StructType([
        StructField("cubicweight", FloatType(), True),
        StructField("height", FloatType(), True),
        StructField("length", FloatType(), True),
        StructField("weight", FloatType(), True),
        StructField("width", FloatType(), True)
    ]), True),
    StructField("offeringInfo", StringType(), True),
    StructField("offeringType", StringType(), True),
    StructField("offeringTypeId", StringType(), True)
])

price_definition_schema = StructType([
    StructField("sellingPrices", ArrayType(StructType([
        StructField("value", LongType(), True),
        StructField("quantity", IntegerType(), True)
    ])), True),
    StructField("calculatedSellingPrice", LongType(), True),
    StructField("total", LongType(), True),
    StructField("reason", StringType(), True)
])

component_schema = StructType([
    StructField("uniqueId", StringType(), True),
    StructField("id", StringType(), True),
    StructField("productId", StringType(), True),
    StructField("ean", StringType(), True),
    StructField("lockId", StringType(), True),
    StructField("itemAttachment", StructType([
        StructField("content", MapType(StringType(), StringType()), True),
        StructField("name", StringType(), True)
    ]), True),
    StructField("attachments", ArrayType(StringType()), True),
    StructField("quantity", IntegerType(), True),
    StructField("seller", StringType(), True),
    StructField("name", StringType(), True),
    StructField("refId", StringType(), True),
    StructField("price", LongType(), True),
    StructField("listPrice", LongType(), True),
    StructField("manualPrice", LongType(), True),
    StructField("priceTags", ArrayType(StringType()), True),
    StructField("imageUrl", StringType(), True),
    StructField("detailUrl", StringType(), True),
    StructField("components", ArrayType(StringType()), True),
    StructField("bundleItems", ArrayType(StringType()), True),
    StructField("params", ArrayType(StringType()), True),
    StructField("offerings", ArrayType(StringType()), True),
    StructField("attachmentOfferings", ArrayType(StringType()), True),
    StructField("sellerSku", StringType(), True),
    StructField("priceValidUntil", StringType(), True),
    StructField("commission", IntegerType(), True),
    StructField("tax", IntegerType(), True),
    StructField("preSaleDate", StringType(), True),
    StructField("additionalInfo", additional_info_schema, True),
    StructField("measurementUnit", StringType(), True),
    StructField("unitMultiplier", FloatType(), True),
    StructField("sellingPrice", LongType(), True),
    StructField("isGift", BooleanType(), True),
    StructField("shippingPrice", LongType(), True),
    StructField("rewardValue", LongType(), True),
    StructField("freightCommission", LongType(), True),
    StructField("priceDefinition", price_definition_schema, True),
    StructField("taxCode", StringType(), True),
    StructField("parentItemIndex", StringType(), True),
    StructField("parentAssemblyBinding", StringType(), True),
    StructField("callCenterOperator", StringType(), True),
    StructField("serialNumbers", StringType(), True),
    StructField("assemblies", ArrayType(StringType()), True),
    StructField("costPrice", LongType(), True)
])
schema = StructType([
    StructField("orderId", StringType(), True),
    StructField("sequence", StringType(), True),
    StructField("marketplaceOrderId", StringType(), True),
    StructField("marketplaceServicesEndpoint", StringType(), True),
    StructField("sellerOrderId", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("affiliateId", StringType(), True),
    StructField("salesChannel", StringType(), True),
    StructField("merchantName", StringType(), True),
    StructField("status", StringType(), True),
    StructField("workflowIsInError", BooleanType(), True),
    StructField("statusDescription", StringType(), True),
    StructField("value", IntegerType(), True),
    StructField("creationDate", TimestampType(), True),
    StructField("lastChange", TimestampType(), True),
    StructField("orderGroup", StringType(), True),
    StructField("totals", ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("value", IntegerType(), True)
    ])), True),
    StructField("items", ArrayType(StructType([
        StructField("uniqueId", StringType(), True),
        StructField("id", StringType(), True),
        StructField("productId", StringType(), True),
        StructField("ean", StringType(), True),
        StructField("lockId", StringType(), True),
        StructField("itemAttachment", StructType([
            StructField("content", MapType(StringType(), StringType()), True),
            StructField("name", StringType(), True)
        ]), True),
        StructField("attachments", ArrayType(array_schema), True),
        StructField("quantity", IntegerType(), True),
        StructField("seller", StringType(), True),
        StructField("name", StringType(), True),
        StructField("refId", StringType(), True),
        StructField("price", IntegerType(), True),
        StructField("listPrice", IntegerType(), True),
        StructField("manualPrice", StringType(), True),
        StructField("priceTags", ArrayType(StringType()), True),
        StructField("imageUrl", StringType(), True),
        StructField("detailUrl", StringType(), True),
        StructField("components", ArrayType(component_schema), True),
        StructField("bundleItems", ArrayType(StructType([
        StructField("uniqueId", StringType(), True),
        StructField("id", StringType(), True),
        StructField("productId", StringType(), True),
        StructField("ean", StringType(), True),
        StructField("lockId", StringType(), True),
        StructField("itemAttachment", StructType([
            StructField("content", StructType([]), True),
            StructField("name", StringType(), True)
        ]), True),
        StructField("attachments", ArrayType(StringType()), True),
        StructField("quantity", IntegerType(), True),
        StructField("seller", StringType(), True),
        StructField("name", StringType(), True),
        StructField("refId", StringType(), True),
        StructField("price", LongType(), True),
        StructField("listPrice", LongType(), True),
        StructField("manualPrice", LongType(), True),
        StructField("priceTags", ArrayType(StringType()), True),
        StructField("imageUrl", StringType(), True),
        StructField("detailUrl", StringType(), True),
        StructField("components", ArrayType(StringType()), True),
        StructField("bundleItems", ArrayType(StringType()), True),
        StructField("params", ArrayType(StringType()), True),
        StructField("offerings", ArrayType(StringType()), True),
        StructField("attachmentOfferings", ArrayType(StructType([
            StructField("name", StringType(), True),
            StructField("required", BooleanType(), True),
            StructField("schema", StructType([
                StructField("takeback", StructType([
                    StructField("MaximumNumberOfCharacters", IntegerType(), True),
                    StructField("Domain", ArrayType(StringType()), True)
                ]), True)
            ]), True)
        ])), True),
        StructField("sellerSku", StringType(), True),
        StructField("priceValidUntil", StringType(), True),
        StructField("commission", LongType(), True),
        StructField("tax", LongType(), True),
        StructField("preSaleDate", StringType(), True),
        StructField("additionalInfo", StructType([
            StructField("brandName", StringType(), True),
            StructField("brandId", StringType(), True),
            StructField("categoriesIds", StringType(), True),
            StructField("categories", StringType(), True),
            StructField("productClusterId", StringType(), True),
            StructField("commercialConditionId", StringType(), True),
            StructField("dimension", StructType([
                StructField("cubicweight", DoubleType(), True),
                StructField("height", DoubleType(), True),
                StructField("length", DoubleType(), True),
                StructField("weight", DoubleType(), True),
                StructField("width", DoubleType(), True)
            ]), True),
            StructField("offeringInfo", StringType(), True),
            StructField("offeringType", StringType(), True),
            StructField("offeringTypeId", StringType(), True)
        ]), True),
        StructField("measurementUnit", StringType(), True),
        StructField("unitMultiplier", DoubleType(), True),
        StructField("sellingPrice", LongType(), True),
        StructField("isGift", BooleanType(), True),
        StructField("shippingPrice", LongType(), True),
        StructField("rewardValue", LongType(), True),
        StructField("freightCommission", LongType(), True),
        StructField("priceDefinition", StructType([
            StructField("sellingPrices", ArrayType(StructType([
                StructField("value", LongType(), True),
                StructField("quantity", IntegerType(), True)
            ])), True),
            StructField("calculatedSellingPrice", LongType(), True),
            StructField("total", LongType(), True),
            StructField("reason", StringType(), True)
        ]), True),
        StructField("taxCode", StringType(), True),
        StructField("parentItemIndex", StringType(), True),
        StructField("parentAssemblyBinding", StringType(), True),
        StructField("callCenterOperator", StringType(), True),
        StructField("serialNumbers", StringType(), True),
        StructField("assemblies", ArrayType(StringType()), True),
        StructField("costPrice", LongType(), True)
    ])), True),
        StructField("params", ArrayType(StringType()), True),
        StructField("offerings", ArrayType(StructType([
            StructField("type", StringType(), True),
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("price", IntegerType(), True)
        ])), True),
        StructField("attachmentOfferings", ArrayType(StructType([
            StructField("name", StringType(), True),
            StructField("required", BooleanType(), True),
            StructField("schema", MapType(StringType(), StructType([
                StructField("MaximumNumberOfCharacters", IntegerType(), True),
                StructField("Domain", ArrayType(StringType()), True)
            ])), True)
        ])), True),
        StructField("sellerSku", StringType(), True),
        StructField("priceValidUntil", StringType(), True),
        StructField("commission", IntegerType(), True),
        StructField("tax", IntegerType(), True),
        StructField("preSaleDate", StringType(), True),
        StructField("additionalInfo", StructType([
            StructField("brandName", StringType(), True),
            StructField("brandId", StringType(), True),
            StructField("categoriesIds", StringType(), True),
            StructField("categories", ArrayType(StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True)
            ])), True),
            StructField("productClusterId", StringType(), True),
            StructField("commercialConditionId", StringType(), True),
            StructField("dimension", StructType([
                StructField("cubicweight", FloatType(), True),
                StructField("height", FloatType(), True),
                StructField("length", FloatType(), True),
                StructField("weight", FloatType(), True),
                StructField("width", FloatType(), True)
            ]), True),
            StructField("offeringInfo", StringType(), True),
            StructField("offeringType", StringType(), True),
            StructField("offeringTypeId", StringType(), True)
        ]), True),
        StructField("measurementUnit", StringType(), True),
        StructField("unitMultiplier", FloatType(), True),
        StructField("sellingPrice", IntegerType(), True),
        StructField("isGift", BooleanType(), True),
        StructField("shippingPrice", StringType(), True),
        StructField("rewardValue", IntegerType(), True),
        StructField("freightCommission", IntegerType(), True),
        StructField("priceDefinition", StructType([
            StructField("sellingPrices", ArrayType(StructType([
                StructField("value", IntegerType(), True),
                StructField("quantity", IntegerType(), True)
            ])), True),
            StructField("calculatedSellingPrice", IntegerType(), True),
            StructField("total", IntegerType(), True),
            StructField("reason", StringType(), True)
        ]), True),
        StructField("taxCode", StringType(), True),
        StructField("parentItemIndex", StringType(), True),
        StructField("parentAssemblyBinding", StringType(), True),
        StructField("callCenterOperator", StringType(), True),
        StructField("serialNumbers", StringType(), True),
        StructField("assemblies", ArrayType(StringType()), True),
        StructField("costPrice", IntegerType(), True)
    ])), True),
    StructField("marketplaceItems", ArrayType(StringType()), True),
    StructField("clientProfileData", StructType([
        StructField("id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("lastName", StringType(), True),
        StructField("documentType", StringType(), True),
        StructField("document", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("corporateName", StringType(), True),
        StructField("tradeName", StringType(), True),
        StructField("corporateDocument", StringType(), True),
        StructField("stateInscription", StringType(), True),
        StructField("corporatePhone", StringType(), True),
        StructField("isCorporate", BooleanType(), True),
        StructField("userProfileId", StringType(), True),
        StructField("userProfileVersion", StringType(), True),
        StructField("customerClass", StringType(), True)
    ]), True),
    StructField("giftRegistryData", StringType(), True),
    StructField("marketingData", StructType([
        StructField("id", StringType(), True),
        StructField("utmSource", StringType(), True),
        StructField("utmPartner", StringType(), True),
        StructField("utmMedium", StringType(), True),
        StructField("utmCampaign", StringType(), True),
        StructField("coupon", StringType(), True),
        StructField("utmiCampaign", StringType(), True),
        StructField("utmipage", StringType(), True),
        StructField("utmiPart", StringType(), True),
        StructField("marketingTags", ArrayType(StringType()), True)
    ]), True),
    StructField("ratesAndBenefitsData", StructType([
        StructField("id", StringType(), True),
        StructField("rateAndBenefitsIdentifiers", ArrayType(StructType([
    StructField("description", StringType(), True),
    StructField("featured", BooleanType(), True),
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("matchedParameters", MapType(StringType(), StringType()), True),
    StructField("additionalInfo", StringType(), True)
])), True)
    ]), True),
    StructField("shippingData", StructType([
        StructField("id", StringType(), True),
        StructField("address", StructType([
            StructField("addressType", StringType(), True),
            StructField("receiverName", StringType(), True),
            StructField("addressId", StringType(), True),
            StructField("versionId", StringType(), True),
            StructField("entityId", StringType(), True),
            StructField("postalCode", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("street", StringType(), True),
            StructField("number", StringType(), True),
            StructField("neighborhood", StringType(), True),
            StructField("complement", StringType(), True),
            StructField("reference", StringType(), True),
            StructField("geoCoordinates", ArrayType(FloatType()), True)
        ]), True),
        StructField("logisticsInfo", ArrayType(StructType([
            StructField("itemIndex", IntegerType(), True),
            StructField("selectedSla", StringType(), True),
            StructField("lockTTL", StringType(), True),
            StructField("price", IntegerType(), True),
            StructField("listPrice", IntegerType(), True),
            StructField("sellingPrice", IntegerType(), True),
            StructField("deliveryWindow", StructType([
            StructField("startDateUtc", StringType(), True),
            StructField("endDateUtc", StringType(), True),
            StructField("price", IntegerType(), True)
            ]), True),
            StructField("deliveryCompany", StringType

(), True),
            StructField("shippingEstimate", StringType(), True),
            StructField("shippingEstimateDate", StringType(), True),
            StructField("slas", ArrayType(StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("shippingEstimate", StringType(), True),
                StructField("deliveryWindow", StringType(), True),
                StructField("price", IntegerType(), True),
                StructField("deliveryChannel", StringType(), True),
                StructField("pickupStoreInfo", StructType([
                    StructField("additionalInfo", StringType(), True),
                    StructField("address", StructType([
                        StructField("addressType", StringType(), True),
                        StructField("receiverName", StringType(), True),
                        StructField("addressId", StringType(), True),
                        StructField("versionId", StringType(), True),
                        StructField("entityId", StringType(), True),
                        StructField("postalCode", StringType(), True),
                        StructField("city", StringType(), True),
                        StructField("state", StringType(), True),
                        StructField("country", StringType(), True),
                        StructField("street", StringType(), True),
                        StructField("number", StringType(), True),
                        StructField("neighborhood", StringType(), True),
                        StructField("complement", StringType(), True),
                        StructField("reference", StringType(), True),
                        StructField("geoCoordinates", ArrayType(FloatType()), True)
                    ]), True),
                    StructField("dockId", StringType(), True),
                    StructField("friendlyName", StringType(), True),
                    StructField("isPickupStore", BooleanType(), True)
                ]), True),
                StructField("polygonName", StringType(), True),
                StructField("lockTTL", StringType(), True),
                StructField("pickupPointId", StringType(), True),
                StructField("transitTime", StringType(), True),
                StructField("pickupDistance", FloatType(), True)
            ])), True),
            StructField("shipsTo", ArrayType(StringType()), True),
            StructField("deliveryIds", ArrayType(StructType([
                StructField("courierId", StringType(), True),
                StructField("courierName", StringType(), True),
                StructField("dockId", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("warehouseId", StringType(), True),
                StructField("accountCarrierName", StringType(), True),
                StructField("kitItemDetails", ArrayType(StructType([
                StructField("ItemId", StringType(), True),
                StructField("WarehouseId", StringType(), True)
                ])), True)
            ])), True),
            StructField("deliveryChannels", ArrayType(StructType([
                StructField("id", StringType(), True),
                StructField("stockBalance", IntegerType(), True)
            ])), True),
            StructField("deliveryChannel", StringType(), True),
            StructField("pickupStoreInfo", StructType([
                StructField("additionalInfo", StringType(), True),
                StructField("address", StringType(), True),
                StructField("dockId", StringType(), True),
                StructField("friendlyName", StringType(), True),
                StructField("isPickupStore", BooleanType(), True)
            ]), True),
            StructField("addressId", StringType(), True),
            StructField("versionId", StringType(), True),
            StructField("entityId", StringType(), True),
            StructField("polygonName", StringType(), True),
            StructField("pickupPointId", StringType(), True),
            StructField("transitTime", StringType(), True)
        ])), True),
        StructField("trackingHints", StringType(), True),
        StructField("selectedAddresses", ArrayType(StructType([
            StructField("addressId", StringType(), True),
            StructField("versionId", StringType(), True),
            StructField("entityId", StringType(), True),
            StructField("addressType", StringType(), True),
            StructField("receiverName", StringType(), True),
            StructField("street", StringType(), True),
            StructField("number", StringType(), True),
            StructField("complement", StringType(), True),
            StructField("neighborhood", StringType(), True),
            StructField("postalCode", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("reference", StringType(), True),
            StructField("geoCoordinates", ArrayType(FloatType()), True)
        ])), True)
    ]), True),
    StructField("paymentData", StructType([
        StructField("giftCards", ArrayType(StructType([
    StructField("id", StringType(), True),
    StructField("redemptionCode", StringType(), True),
    StructField("name", StringType(), True),
    StructField("caption", StringType(), True),
    StructField("value", IntegerType(), True),
    StructField("balance", IntegerType(), True),
    StructField("provider", StringType(), True),
    StructField("groupName", StringType(), True),
    StructField("inUse", BooleanType(), True),
    StructField("isSpecialCard", BooleanType(), True)
])), True),
        StructField("transactions", ArrayType(StructType([
            StructField("isActive", BooleanType(), True),
            StructField("transactionId", StringType(), True),
            StructField("merchantName", StringType(), True),
            StructField("payments", ArrayType(StructType([
                StructField("id", StringType(), True),
                StructField("paymentSystem", StringType(), True),
                StructField("paymentSystemName", StringType(), True),
                StructField("value", IntegerType(), True),
                StructField("installments", IntegerType(), True),
                StructField("referenceValue", IntegerType(), True),
                StructField("cardHolder", StringType(), True),
                StructField("cardNumber", StringType(), True),
                StructField("firstDigits", StringType(), True),
                StructField("lastDigits", StringType(), True),
                StructField("cvv2", StringType(), True),
                StructField("expireMonth", StringType(), True),
                StructField("expireYear", StringType(), True),
                StructField("url", StringType(), True),
                StructField("giftCardId", StringType(), True),
                StructField("giftCardName", StringType(), True),
                StructField("giftCardCaption", StringType(), True),
                StructField("redemptionCode", StringType(), True),
                StructField("group", StringType(), True),
                StructField("tid", StringType(), True),
                StructField("dueDate", StringType(), True),
                StructField("connectorResponses", MapType(StringType(), StringType()), True),
                StructField("giftCardProvider", StringType(), True),
                StructField("giftCardAsDiscount", StringType(), True),
                StructField("koinUrl", StringType(), True),
                StructField("accountId", StringType(), True),
                StructField("parentAccountId", StringType(), True),
                StructField("bankIssuedInvoiceIdentificationNumber", StringType(), True),
                StructField("bankIssuedInvoiceIdentificationNumberFormatted", StringType(), True),
                StructField("bankIssuedInvoiceBarCodeNumber", StringType(), True),
                StructField("bankIssuedInvoiceBarCodeType", StringType(), True),
                StructField("billingAddress", StructType([
    StructField("postalCode", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("street", StringType(), True),
    StructField("number", StringType(), True),
    StructField("neighborhood", StringType(), True),
    StructField("complement", StringType(), True),
    StructField("reference", StringType(), True),
    StructField("geoCoordinates", ArrayType(DoubleType()), True)
]), True),
                StructField("paymentOrigin", StringType(), True)
            ])), True)
        ])), True)
    ]), True),
    StructField("packageAttachment", StructType([
        StructField("packages", ArrayType(StringType()), True)
    ]), True),
    StructField("sellers", ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("logo", StringType(), True),
        StructField("fulfillmentEndpoint", StringType(), True)
    ])), True),
    StructField("callCenterOperatorData", StringType(), True),
    StructField("followUpEmail", StringType(), True),
    StructField("lastMessage", StringType(), True),
    StructField("hostname", StringType(), True),
    StructField("invoiceData", invoice_data_schema, True),
    StructField("changesAttachment", StringType(), True),
    StructField("openTextField", StructType([
    StructField("value", StringType(), True)
]), True),
    StructField("roundingError", IntegerType(), True),
    StructField("orderFormId", StringType(), True),
    StructField("commercialConditionData", commercialConditionData_schema, True),
    StructField("isCompleted", BooleanType(), True),
    StructField("customData", StringType(), True),
    StructField("storePreferencesData", StructType([
        StructField("countryCode", StringType(), True),
        StructField("currencyCode", StringType(), True),
        StructField("currencyFormatInfo", StructType([
            StructField("CurrencyDecimalDigits", IntegerType(), True),
            StructField("CurrencyDecimalSeparator", StringType(), True),
            StructField("CurrencyGroupSeparator", StringType(), True),
            StructField("CurrencyGroupSize", IntegerType(), True),
            StructField("StartsWithCurrencySymbol", BooleanType(), True)
        ]), True),
        StructField("currencyLocale", IntegerType(), True),
        StructField("currencySymbol", StringType(), True),
        StructField("timeZone", StringType(), True)
    ]), True),
    StructField("allowCancellation", BooleanType(), True),
    StructField("allowEdition", BooleanType(), True),
    StructField("isCheckedIn", BooleanType(), True),
    StructField("marketplace", StructType([
        StructField("baseURL", StringType(), True),
        StructField("isCertified", StringType(), True),
        StructField("name", StringType(), True)
    ]), True),
    StructField("authorizedDate", TimestampType(), True),
    StructField("invoicedDate", StringType(), True),
    StructField("cancelReason", StringType(), True),
    StructField("itemMetadata", StructType([
        StructField("Items", ArrayType(StructType([
            StructField("Id", StringType(), True),
            StructField("Seller", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("SkuName", StringType(), True),
            StructField("ProductId", StringType(), True),
            StructField("RefId", StringType(), True),
            StructField("Ean", StringType(), True),
            StructField("ImageUrl", StringType(), True),
            StructField("DetailUrl", StringType(), True),
            StructField("AssemblyOptions", ArrayType(StructType([
                StructField("Id", StringType(), True),
                StructField("Name", StringType(), True),
                StructField("Required", BooleanType(), True),
                StructField("InputValues", StructType([
                    StructField("takeback", StructType([
                        StructField("MaximumNumberOfCharacters", IntegerType(), True),
                        StructField("Domain", ArrayType(StringType()), True)
                    ]), True),
                    StructField("garantia-estendida", StructType([
                        StructField("MaximumNumberOfCharacters", IntegerType(), True),
                        StructField("Domain", ArrayType(StringType()), True)
                    ]), True)
                ]), True),
                StructField("Composition", composition_schema, True)
            ])), True)
        ])), True)
    ]), True),
    StructField("subscriptionData", subscriptionData_schema, True),
    StructField("taxData", StringType(), True),
    StructField("checkedInPickupPointId", StringType(), True),
    StructField("cancellationData", cancellationData_schema, True),
    StructField("clientPreferencesData", StructType([
        StructField("locale", StringType(), True),
        StructField("optinNewsLetter", BooleanType(), True)
    ]), True)
])
discount_schema = StructType([
    StructField("priceTag_name", StringType(), True),
    StructField("priceTag_value", FloatType(), True),
    StructField("priceTag_isPercentual", BooleanType(), True),
    StructField("priceTag_identifier", StringType(), True),
    StructField("priceTag_rawValue", FloatType(), True),
    StructField("priceTag_rate", FloatType(), True),
    StructField("priceTag_jurisCode", StringType(), True),
    StructField("priceTag_jurisType", StringType(), True),
    StructField("priceTag_jurisName", StringType(), True)
])

# COMMAND ----------


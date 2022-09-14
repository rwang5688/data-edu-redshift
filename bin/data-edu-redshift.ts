#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { DataEduRedshiftStack } from '../lib/data-edu-redshift-stack';

const app = new cdk.App();
new DataEduRedshiftStack(app, 'DataEduRedshiftStack');
